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

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
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

// SchedulingRoundReport captures the decisions made by the scheduler during one invocation.
type SchedulingRoundReport struct {
	// Time at which the scheduling cycle started.
	Started time.Time
	// Time at which the scheduling cycle finished.
	Finished time.Time
	// Executor for which the scheduler was invoked.
	Executor string
	// Per-queue scheduling reports.
	QueueSchedulingRoundReports map[string]*QueueSchedulingRoundReport
	// Total resources across all clusters available at the start of the scheduling cycle.
	TotalResources schedulerobjects.ResourceList
	// Resources assigned across all queues during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Total number of jobs successfully scheduled in this round.
	NumScheduledJobs int
	// Reason for why the scheduling round finished.
	TerminationReason string
	// Protects everything in this struct.
	mu sync.Mutex
}

func NewSchedulingRoundReport(
	totalResources schedulerobjects.ResourceList,
	priorityFactorByQueue map[string]float64,
	initialResourcesByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
) *SchedulingRoundReport {
	queueSchedulingRoundReports := make(map[string]*QueueSchedulingRoundReport)
	for queue := range priorityFactorByQueue {
		queueSchedulingRoundReports[queue] = NewQueueSchedulingRoundReport(
			priorityFactorByQueue[queue],
			initialResourcesByQueueAndPriority[queue],
		)
	}
	return &SchedulingRoundReport{
		Started:                      time.Now(),
		QueueSchedulingRoundReports:  queueSchedulingRoundReports,
		TotalResources:               totalResources.DeepCopy(),
		ScheduledResourcesByPriority: make(schedulerobjects.QuantityByPriorityAndResourceType),
	}
}

func (report *SchedulingRoundReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Started:\t%s\n", report.Started)
	fmt.Fprintf(w, "Finished:\t%s\n", report.Finished)
	fmt.Fprintf(w, "Duration:\t%s\n", report.Finished.Sub(report.Started))
	fmt.Fprintf(w, "Total capacity:\t%s\n", report.TotalResources.CompactString())
	totalJobsScheduled := 0
	totalResourcesScheduled := make(schedulerobjects.QuantityByPriorityAndResourceType)
	fmt.Fprintf(w, "Total jobs scheduled:\t%d\n", totalJobsScheduled)
	fmt.Fprintf(w, "Total resources scheduled:\t%s\n", totalResourcesScheduled)
	fmt.Fprintf(w, "Termination reason:\t%s\n", report.TerminationReason)
	w.Flush()
	return sb.String()
}

// AddJobSchedulingReport adds a job scheduling report to the report for this invocation of the scheduler.
// If updateTotals is true, automatically updates scheduled resources
func (report *SchedulingRoundReport) AddJobSchedulingReport(r *JobSchedulingReport, isEvictedJob bool) {
	report.mu.Lock()
	defer report.mu.Unlock()
	if !isEvictedJob && r.UnschedulableReason == "" {
		report.ScheduledResourcesByPriority.AddResourceList(
			r.Req.Priority,
			schedulerobjects.ResourceListFromV1ResourceList(r.Req.ResourceRequirements.Requests),
		)
		report.NumScheduledJobs++
	}
	if queueReport := report.QueueSchedulingRoundReports[r.Job.GetQueue()]; queueReport != nil {
		queueReport.AddJobSchedulingReport(r, isEvictedJob)
	}
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (report *SchedulingRoundReport) ClearJobSpecs() {
	report.mu.Lock()
	defer report.mu.Unlock()
	for _, queueSchedulingRoundReport := range report.QueueSchedulingRoundReports {
		queueSchedulingRoundReport.ClearJobSpecs()
	}
}

func (report *SchedulingRoundReport) SuccessfulJobSchedulingReports() []*JobSchedulingReport {
	report.mu.Lock()
	defer report.mu.Unlock()
	reports := make([]*JobSchedulingReport, 0)
	for _, queueSchedulingRoundReport := range report.QueueSchedulingRoundReports {
		for _, jobReport := range queueSchedulingRoundReport.SuccessfulJobSchedulingReports {
			reports = append(reports, jobReport)
		}
	}
	return reports
}

// QueueSchedulingRoundReport captures the decisions made by the scheduler during one invocation
// for a particular queue.
type QueueSchedulingRoundReport struct {
	// These factors influence the fraction of resources assigned to each queue.
	PriorityFactor float64
	// Resources assigned to the queue across all clusters at the start of the scheduling cycle.
	ResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Resources assigned to this queue during this scheduling cycle.
	ScheduledResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType
	// Reports for all successful job scheduling attempts.
	SuccessfulJobSchedulingReports map[uuid.UUID]*JobSchedulingReport
	// Reports for all unsuccessful job scheduling attempts.
	UnsuccessfulJobSchedulingReports map[uuid.UUID]*JobSchedulingReport
	// Total number of jobs successfully scheduled in this round for this queue.
	NumScheduledJobs int
	// Protects the above maps.
	mu sync.Mutex
}

func NewQueueSchedulingRoundReport(priorityFactor float64, initialResourcesByPriority schedulerobjects.QuantityByPriorityAndResourceType) *QueueSchedulingRoundReport {
	if initialResourcesByPriority == nil {
		initialResourcesByPriority = make(schedulerobjects.QuantityByPriorityAndResourceType)
	} else {
		initialResourcesByPriority = initialResourcesByPriority.DeepCopy()
	}
	return &QueueSchedulingRoundReport{
		PriorityFactor:                   priorityFactor,
		ResourcesByPriority:              initialResourcesByPriority,
		ScheduledResourcesByPriority:     make(schedulerobjects.QuantityByPriorityAndResourceType),
		SuccessfulJobSchedulingReports:   make(map[uuid.UUID]*JobSchedulingReport),
		UnsuccessfulJobSchedulingReports: make(map[uuid.UUID]*JobSchedulingReport),
	}
}

// AddJobSchedulingReport adds a job scheduling report to the report for this invocation of the scheduler.
// Automatically updates scheduled resources by calling AddScheduledResources. Is thread-safe.
func (report *QueueSchedulingRoundReport) AddJobSchedulingReport(r *JobSchedulingReport, isEvictedJob bool) {
	report.mu.Lock()
	defer report.mu.Unlock()
	if r.UnschedulableReason == "" {
		// Always update ResourcesByPriority.
		// Since ResourcesByPriority is used to order queues by fraction of fair share.
		rl := report.ResourcesByPriority[r.Req.Priority]
		rl.Add(schedulerobjects.ResourceListFromV1ResourceList(r.Req.ResourceRequirements.Requests))
		report.ResourcesByPriority[r.Req.Priority] = rl

		// Only if the job is not evicted, update ScheduledResourcesByPriority.
		// Since ScheduledResourcesByPriority is used to control per-round scheduling constraints.
		if !isEvictedJob {
			report.SuccessfulJobSchedulingReports[r.JobId] = r
			report.NumScheduledJobs++
			rl := report.ScheduledResourcesByPriority[r.Req.Priority]
			rl.Add(schedulerobjects.ResourceListFromV1ResourceList(r.Req.ResourceRequirements.Requests))
			report.ScheduledResourcesByPriority[r.Req.Priority] = rl
		}
	} else {
		report.UnsuccessfulJobSchedulingReports[r.JobId] = r
	}
}

// ClearJobSpecs zeroes out job specs to reduce memory usage.
func (report *QueueSchedulingRoundReport) ClearJobSpecs() {
	report.mu.Lock()
	defer report.mu.Unlock()
	for _, jobSchedulingReport := range report.SuccessfulJobSchedulingReports {
		jobSchedulingReport.Job = nil
	}
	for _, jobSchedulingReport := range report.UnsuccessfulJobSchedulingReports {
		jobSchedulingReport.Job = nil
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

func (repo *SchedulingReportsRepository) AddSchedulingRoundReport(report *SchedulingRoundReport) {
	for queue, queueSchedulingRoundReport := range report.QueueSchedulingRoundReports {
		repo.AddMany(queue, maps.Values(queueSchedulingRoundReport.SuccessfulJobSchedulingReports))
		repo.AddMany(queue, maps.Values(queueSchedulingRoundReport.UnsuccessfulJobSchedulingReports))
	}
}

func (repo *SchedulingReportsRepository) AddMany(queueName string, reports []*JobSchedulingReport) {
	for _, report := range reports {
		repo.Add(queueName, report)
	}
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
	Job LegacySchedulerJob
	// Scheduling requirements of this job.
	// We currently require that each job contains exactly one pod spec.
	Req *schedulerobjects.PodRequirements
	// Executor this job was attempted to be assigned to.
	ExecutorId string
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
	// Pod scheduling requirements.
	Req *schedulerobjects.PodRequirements
	// Resource type determined by the scheduler to be the hardest to satisfy
	// the scheduling requirements for.
	DominantResourceType string
	// Node the pod was assigned to.
	// If nil, the pod could not be assigned to any Node.
	Node *schedulerobjects.Node
	// Score indicates how well the pod fits on the selected Node.
	Score int
	// Number of Node types that
	NumMatchedNodeTypes int
	// Number of Node types excluded by reason.
	NumExcludedNodeTypesByReason map[string]int
	// Number of nodes excluded by reason.
	NumExcludedNodesByReason map[string]int
	// Set if an error occurred while attempting to schedule this pod.
	Err error
}

func (report *PodSchedulingReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	fmt.Fprintf(w, "Time:\t%s\n", report.Timestamp)
	if report.Node != nil {
		fmt.Fprintf(w, "Node:\t%s\n", report.Node.Id)
	} else {
		fmt.Fprint(w, "Node:\tnone\n")
	}
	fmt.Fprintf(w, "Score:\t%d\n", report.Score)
	fmt.Fprintf(w, "Number of matched Node types:\t%d\n", report.NumMatchedNodeTypes)
	if len(report.NumExcludedNodeTypesByReason) == 0 {
		fmt.Fprint(w, "Excluded Node types:\tnone\n")
	} else {
		fmt.Fprint(w, "Excluded Node types:\n")
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
	fmt.Fprintf(w, "Error:\t%s\n", report.Err)
	w.Flush()
	return sb.String()
}
