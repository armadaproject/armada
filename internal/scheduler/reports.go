package scheduler

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/openconfig/goyang/pkg/indent"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common/armadaerrors"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// SchedulingReportsRepository stores reports on the most recent scheduling attempts.
type SchedulingReportsRepository struct {
	// Scheduling reports for the jobs that were most recently attempted to be scheduled.
	MostRecentJobSchedulingReports *lru.Cache
	// Scheduling reports for the most recently seen queues.
	MostRecentQueueSchedulingReports *lru.Cache
}

func NewSchedulingReportsRepository(maxQueueSchedulingReports, maxJobSchedulingReports int) *SchedulingReportsRepository {
	mostRecentJobSchedulingReports, err := lru.New(maxJobSchedulingReports)
	if err != nil {
		panic(errors.WithStack(err))
	}
	mostRecentQueueSchedulingReports, err := lru.New(maxQueueSchedulingReports)
	if err != nil {
		panic(errors.WithStack(err))
	}
	return &SchedulingReportsRepository{
		MostRecentJobSchedulingReports:   mostRecentJobSchedulingReports,
		MostRecentQueueSchedulingReports: mostRecentQueueSchedulingReports,
	}
}

type SchedulingRoundReport struct {
	// Time at which the scheduling cycle started.
	Started time.Time
	// Time at which the scheduling cycle finished.
	Finished time.Time
	// Executor for which the scheduler was invoked.
	Executor string
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactorByQueue map[string]float64
	// Resources assigned to each queue at the start of the scheduling cycle.
	InitialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType
	// Reports for all successful job scheduling attempts.
	SuccessfulJobSchedulingReportsByQueue map[string]map[uuid.UUID]*JobSchedulingReport
	// Reports for all unsuccessful job scheduling attempts.
	UnsuccessfulJobSchedulingReportsByQueue map[string]map[uuid.UUID]*JobSchedulingReport
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
	// Reason for why the scheduling round finished.
	TerminationReason string
	// Protects ScheduledResourcesByQueueAndPriority and JobSchedulingReportsByQueue.
	mu sync.Mutex
}

func NewSchedulingRoundReport(totalResources schedulerobjects.ResourceList, priorityFactorByQueue map[string]float64) *SchedulingRoundReport {
	return &SchedulingRoundReport{
		Started:                                 time.Now(),
		PriorityFactorByQueue:                   maps.Clone(priorityFactorByQueue),
		InitialResourcesByQueueAndPriority:      make(map[string]schedulerobjects.QuantityByPriorityAndResourceType),
		ScheduledResourcesByQueueAndPriority:    make(map[string]schedulerobjects.QuantityByPriorityAndResourceType),
		SuccessfulJobSchedulingReportsByQueue:   make(map[string]map[uuid.UUID]*JobSchedulingReport),
		UnsuccessfulJobSchedulingReportsByQueue: make(map[string]map[uuid.UUID]*JobSchedulingReport),
		TotalResources:                          totalResources.DeepCopy(),
	}
}

func (report *SchedulingRoundReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Started:\t%s\n", report.Started)
	fmt.Fprintf(w, "Finished:\t%s\n", report.Finished)
	fmt.Fprintf(w, "Duration:\t%s\n", report.Finished.Sub(report.Started))
	fmt.Fprintf(w, "Total capacity:\t%s\n", report.TotalResources.CompactString())
	if len(report.PriorityFactorByQueue) == 0 {
		fmt.Fprint(w, "Queue priority factors:\tnone\n")
	} else {
		fmt.Fprint(w, "Queue priority factors:\n")
		for queue, priorityFactor := range report.PriorityFactorByQueue {
			fmt.Fprintf(w, "  %s: %f\n", queue, priorityFactor)
		}
	}
	totalJobsScheduled := 0
	totalResourcesScheduled := make(schedulerobjects.QuantityByPriorityAndResourceType)
	if len(report.SuccessfulJobSchedulingReportsByQueue) == 0 {
		fmt.Fprint(w, "Jobs scheduled:\tnone\n")
	} else {
		fmt.Fprint(w, "Jobs scheduled:\n")
		for queue, jobReports := range report.SuccessfulJobSchedulingReportsByQueue {
			scheduledResourcesByPriority, ok := report.ScheduledResourcesByQueueAndPriority[queue]
			if ok {
				fmt.Fprintf(w, "  %s: %d (%s)\n", queue, len(jobReports), scheduledResourcesByPriority)
			} else {
				fmt.Fprintf(w, "  %s: %d\n", queue, len(jobReports))
			}
			totalJobsScheduled += len(jobReports)
			totalResourcesScheduled.Add(scheduledResourcesByPriority)
		}
	}
	fmt.Fprintf(w, "Total jobs scheduled:\t%d\n", totalJobsScheduled)
	fmt.Fprintf(w, "Total resources scheduled:\t%s\n", totalResourcesScheduled)
	fmt.Fprintf(w, "Termination reason:\t%s\n", report.TerminationReason)
	w.Flush()
	return sb.String()
}

// Add a job scheduling report to the report for this invocation of the scheduler.
// Automatically updates scheduled resources by calling AddScheduledResources. Is thread-safe.
func (schedulingRoundReport *SchedulingRoundReport) AddJobSchedulingReport(jobSchedulingReport *JobSchedulingReport) {
	schedulingRoundReport.mu.Lock()
	queue := jobSchedulingReport.Job.Queue
	if jobSchedulingReport.UnschedulableReason == "" {
		if m, ok := schedulingRoundReport.SuccessfulJobSchedulingReportsByQueue[queue]; ok {
			m[jobSchedulingReport.JobId] = jobSchedulingReport
		} else {
			schedulingRoundReport.SuccessfulJobSchedulingReportsByQueue[queue] = map[uuid.UUID]*JobSchedulingReport{
				jobSchedulingReport.JobId: jobSchedulingReport,
			}
		}
	} else {
		if m, ok := schedulingRoundReport.UnsuccessfulJobSchedulingReportsByQueue[queue]; ok {
			m[jobSchedulingReport.JobId] = jobSchedulingReport
		} else {
			schedulingRoundReport.UnsuccessfulJobSchedulingReportsByQueue[queue] = map[uuid.UUID]*JobSchedulingReport{
				jobSchedulingReport.JobId: jobSchedulingReport,
			}
		}
	}
	schedulingRoundReport.mu.Unlock()
	if jobSchedulingReport.UnschedulableReason == "" {
		schedulingRoundReport.AddScheduledResources(jobSchedulingReport.Job.Queue, jobSchedulingReport.Req)
	}
}

// Add scheduled resources for a queue.
// Called by AddJobSchedulingReport.
func (schedulingRoundReport *SchedulingRoundReport) AddScheduledResources(queue string, req *schedulerobjects.PodRequirements) {
	schedulingRoundReport.mu.Lock()
	defer schedulingRoundReport.mu.Unlock()
	m, ok := schedulingRoundReport.ScheduledResourcesByQueueAndPriority[queue]
	if !ok {
		m = make(schedulerobjects.QuantityByPriorityAndResourceType)
		schedulingRoundReport.ScheduledResourcesByQueueAndPriority[queue] = m
	}
	rl, ok := m[req.Priority]
	if !ok {
		rl.Resources = make(map[string]resource.Quantity)
	}
	for resource, quantity := range req.ResourceRequirements.Requests {
		q := rl.Resources[string(resource)]
		q.Add(quantity)
		rl.Resources[string(resource)] = q
	}
	m[req.Priority] = rl
}

// Zero out any job specs stored in job scheduling reports referenced from this round report,
// to reduce memory usage.
func (schedulingRoundReport *SchedulingRoundReport) ClearJobSpecs() {
	schedulingRoundReport.mu.Lock()
	defer schedulingRoundReport.mu.Unlock()
	for _, jobSchedulingReports := range schedulingRoundReport.SuccessfulJobSchedulingReportsByQueue {
		for _, jobSchedulingReport := range jobSchedulingReports {
			jobSchedulingReport.Job = nil
		}
	}
	for _, jobSchedulingReports := range schedulingRoundReport.UnsuccessfulJobSchedulingReportsByQueue {
		for _, jobSchedulingReport := range jobSchedulingReports {
			jobSchedulingReport.Job = nil
		}
	}
}

func (repo *SchedulingReportsRepository) GetQueueReport(ctx context.Context, queue *schedulerobjects.Queue) (*schedulerobjects.QueueReport, error) {
	report, ok := repo.GetQueueSchedulingReport(queue.Name)
	if !ok {
		return nil, &armadaerrors.ErrNotFound{
			Type:    "QueueSchedulingReport",
			Value:   queue.Name,
			Message: "this queue has not been considered for scheduling recently",
		}
	}
	return &schedulerobjects.QueueReport{
		Report: report.String(),
	}, nil
}

func (repo *SchedulingReportsRepository) GetJobReport(ctx context.Context, jobId *schedulerobjects.JobId) (*schedulerobjects.JobReport, error) {
	jobUuid, err := uuidFromUlidString(jobId.Id)
	if err != nil {
		return nil, err
	}
	report, ok := repo.GetJobSchedulingReport(jobUuid)
	if !ok {
		return nil, &armadaerrors.ErrNotFound{
			Type:    "JobSchedulingReport",
			Value:   jobId.Id,
			Message: "this job has not been considered for scheduling recently",
		}
	}
	return &schedulerobjects.JobReport{
		Report: report.String(),
	}, nil
}

func (repo *SchedulingReportsRepository) Add(queueName string, report *JobSchedulingReport) {
	repo.MostRecentJobSchedulingReports.Add(report.JobId, report)
	if value, ok := repo.MostRecentQueueSchedulingReports.Get(queueName); ok {
		queueReport := value.(*QueueSchedulingReport)
		if report.UnschedulableReason == "" {
			queueReport.MostRecentSuccessfulJobSchedulingReport = report
		} else {
			queueReport.MostRecentUnsuccessfulJobSchedulingReport = report
		}
	} else {
		queueReport := &QueueSchedulingReport{
			Name: queueName,
		}
		if report.UnschedulableReason == "" {
			queueReport.MostRecentSuccessfulJobSchedulingReport = report
		} else {
			queueReport.MostRecentUnsuccessfulJobSchedulingReport = report
		}
		repo.MostRecentQueueSchedulingReports.Add(queueName, queueReport)
	}
}

func (repo *SchedulingReportsRepository) GetQueueSchedulingReport(queueName string) (*QueueSchedulingReport, bool) {
	if value, ok := repo.MostRecentQueueSchedulingReports.Get(queueName); ok {
		report := value.(*QueueSchedulingReport)
		return report, true
	} else {
		return nil, false
	}
}

func (repo *SchedulingReportsRepository) GetJobSchedulingReport(jobId uuid.UUID) (*JobSchedulingReport, bool) {
	if value, ok := repo.MostRecentJobSchedulingReports.Get(jobId); ok {
		report := value.(*JobSchedulingReport)
		return report, true
	} else {
		return nil, false
	}
}

// QueueSchedulingReport contains job scheduling reports for the most
// recent successful and failed scheduling attempts for this queue.
type QueueSchedulingReport struct {
	// Queue name.
	Name                                      string
	MostRecentSuccessfulJobSchedulingReport   *JobSchedulingReport
	MostRecentUnsuccessfulJobSchedulingReport *JobSchedulingReport
}

func (report *QueueSchedulingReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Queue:\t%s\n", report.Name)
	if report.MostRecentSuccessfulJobSchedulingReport != nil {
		fmt.Fprint(w, "Most recent successful scheduling attempt:\n")
		fmt.Fprint(w, indent.String("\t", report.MostRecentSuccessfulJobSchedulingReport.String()))
	} else {
		fmt.Fprint(w, "Most recent successful scheduling attempt:\tnone\n")
	}
	if report.MostRecentUnsuccessfulJobSchedulingReport != nil {
		fmt.Fprint(w, "Most recent unsuccessful scheduling attempt:\n")
		fmt.Fprint(w, indent.String("\t", report.MostRecentUnsuccessfulJobSchedulingReport.String()))
	} else {
		fmt.Fprint(w, "Most recent unsuccessful scheduling attempt:\n")
	}
	w.Flush()
	return sb.String()
}

// JobSchedulingReport is created by the scheduler and contains information
// about the decision made by the scheduler for this job.
type JobSchedulingReport struct {
	// Time at which this report was created.
	Timestamp time.Time
	// Id of the job this pod corresponds to.
	JobId uuid.UUID
	// Job spec.
	Job *api.Job
	// Scheduling requirements of this job.
	// We currently require that each job contains exactly one pod spec.
	Req *schedulerobjects.PodRequirements
	// Executor this job was assigned to.
	// If empty, the job could not be scheduled,
	// and the UnschedulableReason is populated.
	ExecutorId string
	// Resources assigned to this queue during this invocation of the scheduler,
	// if the job were to be scheduled.
	//
	// TODO: Only store in the scheduler round report.
	RoundQueueResources schedulerobjects.ResourceList
	// Total Resources assigned to this queue,
	// if the job were to be scheduled.
	//
	// TODO: Only store in the scheduler round report.
	TotalQueueResources schedulerobjects.ResourceList
	// Total Resources assigned to this queue by priority,
	// if the job were to be scheduled.
	//
	// TODO: Only store in the scheduler round report.
	TotalQueueResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Reason for why the job could not be scheduled.
	// Empty if the job was scheduled successfully.
	UnschedulableReason string
	// Scheduling reports for the individual pods that make up the job.
	PodSchedulingReports []*PodSchedulingReport
}

func (report *JobSchedulingReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", report.Timestamp)
	if jobId, err := armadaevents.UlidStringFromProtoUuid(
		armadaevents.ProtoUuidFromUuid(report.JobId),
	); err == nil {
		fmt.Fprintf(w, "Job id:\t%s\n", jobId)
	} else {
		fmt.Fprintf(w, "Job id:\t%s\n", err)
	}
	if report.ExecutorId != "" {
		fmt.Fprintf(w, "Executor:\t%s\n", report.ExecutorId)
	} else {
		fmt.Fprint(w, "Executor:\tnone\n")
	}
	if report.UnschedulableReason != "" {
		fmt.Fprintf(w, "UnschedulableReason:\t%s\n", report.UnschedulableReason)
	} else {
		fmt.Fprint(w, "UnschedulableReason:\tnone\n")
	}
	if len(report.PodSchedulingReports) == 0 {
		fmt.Fprint(w, "Pod scheduling reports:\tnone\n")
	} else {
		fmt.Fprint(w, "Pod scheduling reports:\n")
	}
	for _, podSchedulingReport := range report.PodSchedulingReports {
		fmt.Fprint(w, indent.String("\t", podSchedulingReport.String()))
	}
	w.Flush()
	return sb.String()
}

// PodSchedulingReport is returned by SelectAndBindNodeToPod and
// contains detailed information on the scheduling decision made for this pod.
type PodSchedulingReport struct {
	// Time at which this report was created.
	Timestamp time.Time
	// Id of the job this pod corresponds to.
	JobId uuid.UUID
	// Pod scheduling requirements.
	Req *schedulerobjects.PodRequirements
	// Resource type determined by the scheduler to be the hardest to satisfy
	// the scheduling requirements for.
	DominantResourceType string
	// Node the pod was assigned to.
	// If nil, the pod could not be assigned to any node.
	Node *schedulerobjects.Node
	// Score indicates how well the pod fits on the selected node.
	Score int
	// Number of node types that
	NumMatchedNodeTypes int
	// Number of node types excluded by reason.
	NumExcludedNodeTypesByReason map[string]int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
}

func (report *PodSchedulingReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", report.Timestamp)
	if jobId, err := armadaevents.UlidStringFromProtoUuid(
		armadaevents.ProtoUuidFromUuid(report.JobId),
	); err == nil {
		fmt.Fprintf(w, "Job id:\t%s\n", jobId)
	} else {
		fmt.Fprintf(w, "Job id:\t%s\n", err)
	}
	if report.Node != nil {
		fmt.Fprintf(w, "Node:\t%s\n", report.Node.Id)
	} else {
		fmt.Fprint(w, "Node:\tnone\n")
	}
	fmt.Fprintf(w, "Score:\t%d\n", report.Score)
	fmt.Fprintf(w, "Number of matched node types:\t%d\n", report.NumMatchedNodeTypes)
	if len(report.NumExcludedNodeTypesByReason) == 0 {
		fmt.Fprint(w, "Excluded node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded node types:\n")
		for reason, count := range report.NumExcludedNodeTypesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
	}
	requestForDominantResourceType := report.Req.ResourceRequirements.Requests[v1.ResourceName(report.DominantResourceType)]
	fmt.Fprint(w, "Excluded nodes:\n")
	if len(report.NumExcludedNodesByReason) == 0 && requestForDominantResourceType.IsZero() {
		fmt.Fprint(w, "Number of excluded nodes:\tnone\n")
	} else {
		for reason, count := range report.NumExcludedNodesByReason {
			fmt.Fprintf(w, "\t%d:\t%s\n", count, reason)
		}
		fmt.Fprintf(
			w,
			"\tany nodes with less than %s %s available at priority %d\n",
			requestForDominantResourceType.String(),
			report.DominantResourceType,
			report.Req.Priority,
		)
	}
	w.Flush()
	return sb.String()
}
