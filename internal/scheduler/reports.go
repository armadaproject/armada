package scheduler

import (
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
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

func (repo *SchedulingReportsRepository) Add(queueName string, report *JobSchedulingReport) {
	repo.MostRecentJobSchedulingReports.Add(report.JobId, report)
	if value, ok := repo.MostRecentQueueSchedulingReports.Get(queueName); ok {
		queueReport := value.(*QueueSchedulingReport)
		if report.ExecutorId != "" {
			queueReport.MostRecentSuccessfulJobSchedulingReport = report
		} else {
			queueReport.MostRecentUnsuccessfulJobSchedulingReport = report
		}
	} else {
		queueReport := &QueueSchedulingReport{
			Name: queueName,
		}
		if report.ExecutorId != "" {
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

// JobSchedulingReport is created by the scheduler and contains information
// about the decision made by the scheduler for this job.
type JobSchedulingReport struct {
	// Time at which this report was created.
	Timestamp time.Time
	// Id of the job this pod corresponds to.
	JobId uuid.UUID
	// Executor this job was assigned to.
	// If empty, the job could not be scheduled,
	// and the UnschedulableReason is populated.
	ExecutorId string
	// Reason for why the job could not be scheduled.
	// Empty if the job was scheduled successfully.
	UnschedulableReason string
	// Reports for the individual pods that make up the job.
	PodSchedulingReports []*PodSchedulingReport
}

func (report *JobSchedulingReport) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 1, 1, 1, ' ', 0)
	if jobId, err := armadaevents.UlidStringFromProtoUuid(
		armadaevents.ProtoUuidFromUuid(report.JobId),
	); err == nil {
		fmt.Fprintf(w, "Job id:\t%s\n", jobId)
	} else {
		fmt.Fprintf(w, "Job id:\t%s\n", err)
	}
	fmt.Fprintf(w, "Time:\t%s\n", report.Timestamp)
	if report.ExecutorId != "" {
		fmt.Fprintf(w, "Executor:\t%s\n", report.ExecutorId)
	} else {
		fmt.Fprint(w, "Executor:\tnone\n")
	}
	if report.ExecutorId != "" {
		fmt.Fprintf(w, "UnschedulableReason:\t%s\n", report.UnschedulableReason)
	} else {
		fmt.Fprint(w, "UnschedulableReason:\tnone\n")
	}
	if len(report.PodSchedulingReports) == 0 {
		fmt.Fprint(w, "Pod scheduling reports:\tnone\n")
	} else {
		fmt.Fprint(w, "Pod scheduling reports:\n")
	}
	for _, podSchedulingReports := range report.PodSchedulingReports {
		fmt.Fprint(w, podSchedulingReports.String())
	}
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
	if jobId, err := armadaevents.UlidStringFromProtoUuid(
		armadaevents.ProtoUuidFromUuid(report.JobId),
	); err == nil {
		fmt.Fprintf(w, "Job id:\t%s\n", jobId)
	} else {
		fmt.Fprintf(w, "Job id:\t%s\n", err)
	}
	fmt.Fprintf(w, "Time:\t%s\n", report.Timestamp)
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
			"\tand any nodes with less than %s %s available at priority %d\n",
			requestForDominantResourceType.String(),
			report.DominantResourceType,
			report.Req.Priority,
		)
	}
	w.Flush()
	return sb.String()
}
