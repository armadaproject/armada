package reports

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/gogo/status"
	"github.com/oklog/ulid"
	"github.com/openconfig/goyang/pkg/indent"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	internalschedulerobjects "github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api/schedulerobjects"
)

// executorRepository is the part of the scheduler's executor repository the reports server reads from.
type executorRepository interface {
	GetExecutors(ctx *armadacontext.Context) ([]*internalschedulerobjects.Executor, error)
	GetExecutorSettings(ctx *armadacontext.Context) ([]*internalschedulerobjects.ExecutorSettings, error)
}

type Server struct {
	repository         *SchedulingContextRepository
	executorRepository executorRepository
}

func NewServer(repository *SchedulingContextRepository, executorRepository executorRepository) *Server {
	return &Server{
		repository:         repository,
		executorRepository: executorRepository,
	}
}

func (s *Server) GetSchedulingReport(_ context.Context, request *schedulerobjects.SchedulingReportRequest) (*schedulerobjects.SchedulingReport, error) {
	var report string
	verbosity := request.GetVerbosity()
	switch filter := request.GetFilter().(type) {
	case *schedulerobjects.SchedulingReportRequest_MostRecentForQueue:
		queueName := strings.TrimSpace(filter.MostRecentForQueue.GetQueueName())
		report = s.getQueueReportString(queueName, verbosity)
	case *schedulerobjects.SchedulingReportRequest_MostRecentForJob:
		jobId := strings.TrimSpace(filter.MostRecentForJob.GetJobId())
		report = s.getJobReportString(jobId)
	default:
		report = s.getSchedulingReportString(verbosity)
	}
	return &schedulerobjects.SchedulingReport{Report: report}, nil
}

func (s *Server) GetQueueReport(ctx context.Context, request *schedulerobjects.QueueReportRequest) (*schedulerobjects.QueueReport, error) {
	queueName := strings.TrimSpace(request.GetQueueName())
	verbosity := request.GetVerbosity()
	return &schedulerobjects.QueueReport{
		Report: s.getQueueReportString(queueName, verbosity),
	}, nil
}

func (s *Server) GetJobReport(ctx context.Context, request *schedulerobjects.JobReportRequest) (*schedulerobjects.JobReport, error) {
	jobId := strings.TrimSpace(request.GetJobId())
	if _, err := ulid.Parse(jobId); err != nil {
		return nil, status.Newf(codes.InvalidArgument, "%s is not a valid jobId", request.GetJobId()).Err()
	}
	return &schedulerobjects.JobReport{
		Report: s.getJobReportString(jobId),
	}, nil
}

// GetExecutors returns the executors stored in the scheduler database, merged with their executor settings.
// Executors are sorted by name. If request.Name is set, only that executor is returned, or NotFound if it is unknown.
func (s *Server) GetExecutors(grpcCtx context.Context, request *schedulerobjects.ExecutorsRequest) (*schedulerobjects.ExecutorsResponse, error) {
	if s.executorRepository == nil {
		return nil, status.Error(codes.Unimplemented, "executor information is not available")
	}
	ctx := armadacontext.FromGrpcCtx(grpcCtx)
	executors, err := s.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting executors: %s", err)
	}
	settings, err := s.executorRepository.GetExecutorSettings(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error getting executor settings: %s", err)
	}
	settingsByExecutor := make(map[string]*internalschedulerobjects.ExecutorSettings, len(settings))
	for _, es := range settings {
		settingsByExecutor[es.ExecutorId] = es
	}

	name := strings.TrimSpace(request.GetName())
	summaries := make([]*schedulerobjects.ExecutorSummary, 0, len(executors))
	for _, executor := range executors {
		if name != "" && executor.Id != name {
			continue
		}
		summary := &schedulerobjects.ExecutorSummary{
			Name:           executor.Id,
			Pool:           executor.Pool,
			LastUpdateTime: executor.LastUpdateTime,
			NodeCount:      int32(len(executor.Nodes)),
		}
		if es, ok := settingsByExecutor[executor.Id]; ok {
			summary.Cordoned = es.Cordoned
			summary.CordonReason = es.CordonReason
			summary.CordonedBy = es.SetByUser
		}
		summaries = append(summaries, summary)
	}
	if name != "" && len(summaries) == 0 {
		return nil, status.Errorf(codes.NotFound, "executor %s not found", name)
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].Name < summaries[j].Name })
	return &schedulerobjects.ExecutorsResponse{Executors: summaries}, nil
}

func (s *Server) getQueueReportString(queue string, verbosity int32) string {
	poolCtxts := s.repository.QueueSchedulingContext(queue)
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, poolCtx := range poolCtxts {
		fmt.Fprintf(w, "%s:\n", poolCtx.pool)
		if poolCtx.schedulingCtx != nil {
			fmt.Fprintf(w, "\tMost recent scheduling round that considered queue %s:\n", queue)
			fmt.Fprint(w, indent.String("\t\t", poolCtx.schedulingCtx.ReportString(verbosity)))
		} else {
			fmt.Fprintf(w, "\tMost recent scheduling round that considered queue %s: none\n", queue)
		}
	}
	w.Flush()
	return sb.String()
}

func (s *Server) getJobReportString(jobId string) string {
	poolCtxts := s.repository.JobSchedulingContext(jobId)
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, poolCtx := range poolCtxts {
		fmt.Fprintf(w, "%s:\n", poolCtx.pool)
		if poolCtx.schedulingCtx != nil {
			fmt.Fprintf(w, "\tMost recent scheduling round that affected job %s:\n", jobId)
			fmt.Fprint(w, indent.String("\t\t", poolCtx.schedulingCtx.String()))
		} else {
			fmt.Fprintf(w, "\tMost recent scheduling round that affected job %s: none\n", jobId)
		}
	}
	w.Flush()
	return sb.String()
}

func (s *Server) getSchedulingReportString(verbosity int32) string {
	poolCtxts := s.repository.RoundSchedulingContext()
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	for _, poolCtx := range poolCtxts {
		fmt.Fprintf(w, "%s:\n", poolCtx.pool)
		fmt.Fprint(w, indent.String("\t", "Most recent scheduling round:\n"))
		fmt.Fprint(w, indent.String("\t\t", poolCtx.schedulingCtx.ReportString(verbosity)))
	}
	w.Flush()
	return sb.String()
}
