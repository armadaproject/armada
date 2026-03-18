package db

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/lookout/model"
	"github.com/armadaproject/armada/pkg/api"
)

type Database interface {
	InitialiseSchema(ctx context.Context) error
	ExecuteIngestionQueryBatch(ctx context.Context, queries []IngestionQuery) error
	GetJobRunDebugMessage(ctx context.Context, jobRunID string) (string, error)
	GetJobRunError(ctx context.Context, jobRunID string) (string, error)
	GetJobSpec(ctx context.Context, jobID string) (*api.Job, error)
	GetJobGroups(ctx *context.Context, filters []*model.Filter, order *model.Order, groupedField *model.GroupedField, aggregates []string, skip int, take int) ([]*model.JobGroup, error)
	GetJobs(ctx *context.Context, filters []*model.Filter, activeJobSets bool, order *model.Order, skip int, take int) ([]*model.Job, error)
	PopulateHistoricalJobs(ctx context.Context, params HistoricalJobsParams) error
	TearDown(ctx context.Context) error
	Close()
}

// HistoricalJobsParams describes a batch of terminal historical jobs to insert.
//
// The threshold fields use a scale of 1000: a job with index i is assigned a
// state based on i%1000. Jobs where i%1000 < SucceededThreshold are succeeded,
// i%1000 < ErroredThreshold are errored, i%1000 < CancelledThreshold are
// cancelled, and the remainder are preempted. Derive them as:
//
//	SucceededThreshold = int(ProportionSucceeded * 1000)
//	ErroredThreshold   = SucceededThreshold + int(ProportionErrored * 1000)
//	CancelledThreshold = ErroredThreshold   + int(ProportionCancelled * 1000)
type HistoricalJobsParams struct {
	QueueIdx           int
	JobSetIdx          int
	QueueName          string
	JobSetName         string
	NJobs              int
	ChunkSize          int
	SucceededThreshold int
	ErroredThreshold   int
	CancelledThreshold int
	JobAgeDays         []int
	JobSpecBytes       []byte
	ErrorBytes         []byte
	DebugBytes         []byte
	PreemptionBytes    []byte
}

type IngestionQuery interface {
	isIngestionQuery()
}

// JobIDFromQuery extracts the job ID from any IngestionQuery. For run-based
// queries, it extracts the job ID prefix from the run ID.
func JobIDFromQuery(q IngestionQuery) string {
	switch v := q.(type) {
	case InsertJob:
		return v.Job.JobID
	case UpdateJobPriority:
		return v.JobID
	case SetJobCancelled:
		return v.JobID
	case SetJobSucceeded:
		return v.JobID
	case InsertJobError:
		return v.JobID
	case SetJobPreempted:
		return v.JobID
	case SetJobRejected:
		return v.JobID
	case SetJobErrored:
		return v.JobID
	case SetJobRunning:
		return v.JobID
	case SetJobPending:
		return v.JobID
	case SetJobLeased:
		return v.JobID
	case InsertJobRun:
		return v.JobID
	case SetJobRunStarted:
		return jobIDFromRunID(v.JobRunID)
	case SetJobRunPending:
		return jobIDFromRunID(v.JobRunID)
	case SetJobRunCancelled:
		return jobIDFromRunID(v.JobRunID)
	case SetJobRunFailed:
		return jobIDFromRunID(v.JobRunID)
	case SetJobRunSucceeded:
		return jobIDFromRunID(v.JobRunID)
	case SetJobRunPreempted:
		return jobIDFromRunID(v.JobRunID)
	default:
		return ""
	}
}

func jobIDFromRunID(runID string) string {
	if len(runID) >= 18 {
		return runID[:18]
	}
	return runID
}

type InsertJob struct {
	Job     *NewJob
	JobSpec []byte
}

func (InsertJob) isIngestionQuery() {}

type NewJob struct {
	JobID            string
	Queue            string
	JobSet           string
	Owner            string
	Namespace        string
	Priority         int64
	PriorityClass    string
	Submitted        time.Time
	Cpu              int64
	Memory           int64
	EphemeralStorage int64
	Gpu              int64
	Annotations      map[string]string
}

type UpdateJobPriority struct {
	JobID     string
	Priority  int64
	Submitted time.Time
}

func (UpdateJobPriority) isIngestionQuery() {}

type SetJobCancelled struct {
	JobID        string
	Time         time.Time
	CancelReason string
	CancelUser   string
	Submitted    time.Time
}

func (SetJobCancelled) isIngestionQuery() {}

type SetJobSucceeded struct {
	JobID     string
	Time      time.Time
	Submitted time.Time
}

func (SetJobSucceeded) isIngestionQuery() {}

type InsertJobError struct {
	JobID     string
	Error     []byte
	Submitted time.Time
}

func (InsertJobError) isIngestionQuery() {}

type SetJobPreempted struct {
	JobID     string
	Time      time.Time
	Submitted time.Time
}

func (SetJobPreempted) isIngestionQuery() {}

type SetJobRejected struct {
	JobID     string
	Time      time.Time
	Submitted time.Time
}

func (SetJobRejected) isIngestionQuery() {}

type SetJobErrored struct {
	JobID     string
	Time      time.Time
	Submitted time.Time
}

func (SetJobErrored) isIngestionQuery() {}

type SetJobRunning struct {
	JobID       string
	Time        time.Time
	LatestRunID string
	Submitted   time.Time
}

func (SetJobRunning) isIngestionQuery() {}

type SetJobRunStarted struct {
	JobRunID  string
	Time      time.Time
	Node      string
	Submitted time.Time
}

func (SetJobRunStarted) isIngestionQuery() {}

type SetJobPending struct {
	JobID     string
	Time      time.Time
	RunID     string
	Submitted time.Time
}

func (SetJobPending) isIngestionQuery() {}

type SetJobRunPending struct {
	JobRunID  string
	Time      time.Time
	Submitted time.Time
}

func (SetJobRunPending) isIngestionQuery() {}

type SetJobRunCancelled struct {
	JobRunID  string
	Time      time.Time
	Submitted time.Time
}

func (SetJobRunCancelled) isIngestionQuery() {}

type SetJobRunFailed struct {
	JobRunID  string
	Time      time.Time
	Error     []byte
	Debug     []byte
	ExitCode  int32
	Submitted time.Time
}

func (SetJobRunFailed) isIngestionQuery() {}

type SetJobRunSucceeded struct {
	JobRunID  string
	Time      time.Time
	ExitCode  int32
	Submitted time.Time
}

func (SetJobRunSucceeded) isIngestionQuery() {}

type SetJobRunPreempted struct {
	JobRunID  string
	Time      time.Time
	Error     []byte
	Submitted time.Time
}

func (SetJobRunPreempted) isIngestionQuery() {}

type SetJobLeased struct {
	JobID     string
	Time      time.Time
	RunID     string
	Submitted time.Time
}

func (SetJobLeased) isIngestionQuery() {}

type InsertJobRun struct {
	JobRunID  string
	JobID     string
	Cluster   string
	Node      string
	Pool      string
	Time      time.Time
	Submitted time.Time
}

func (InsertJobRun) isIngestionQuery() {}
