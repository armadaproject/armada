package reports

import (
	"context"
	"fmt"
	"strings"
	"text/tabwriter"

	"github.com/gogo/status"
	"github.com/oklog/ulid"
	"github.com/openconfig/goyang/pkg/indent"
	"google.golang.org/grpc/codes"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

type Server struct {
	repository *SchedulingContextRepository
}

func NewServer(repository *SchedulingContextRepository) *Server {
	return &Server{
		repository: repository,
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
