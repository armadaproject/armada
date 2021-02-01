package repository

import (
	"context"
	"database/sql"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/lib/pq"

	"github.com/G-Research/armada/pkg/api/lookout"
)

// Emulates JobStates enum
// can't use protobuf enums because gogoproto + grpc-gateway is hard with K8s specific messages
type jobStates struct {
	Queued    string
	Pending   string
	Running   string
	Succeeded string
	Failed    string
	Cancelled string
}

type JobRepository interface {
	GetQueueInfos(ctx context.Context) ([]*lookout.QueueInfo, error)
	GetJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error)
	GetJobs(ctx context.Context, opts *lookout.GetJobsRequest) ([]*lookout.JobInfo, error)
}

type SQLJobRepository struct {
	goquDb *goqu.Database
	clock  Clock
}

var (
	// Tables
	jobTable    = goqu.T("job")
	jobRunTable = goqu.T("job_run")

	// Columns: job table
	job_jobId     = goqu.I("job.job_id")
	job_queue     = goqu.I("job.queue")
	job_owner     = goqu.I("job.owner")
	job_jobset    = goqu.I("job.jobset")
	job_priority  = goqu.I("job.priority")
	job_submitted = goqu.I("job.submitted")
	job_cancelled = goqu.I("job.cancelled")
	job_job       = goqu.I("job.job")

	// Columns: job_run table
	jobRun_runId            = goqu.I("job_run.run_id")
	jobRun_jobId            = goqu.I("job_run.job_id")
	jobRun_cluster          = goqu.I("job_run.cluster")
	jobRun_node             = goqu.I("job_run.node")
	jobRun_created          = goqu.I("job_run.created")
	jobRun_started          = goqu.I("job_run.started")
	jobRun_finished         = goqu.I("job_run.finished")
	jobRun_succeeded        = goqu.I("job_run.succeeded")
	jobRun_error            = goqu.I("job_run.error")
	jobRun_unableToSchedule = goqu.I("job_run.unable_to_schedule")
)

type JobRow struct {
	JobId     sql.NullString  `db:"job_id"`
	Queue     sql.NullString  `db:"queue"`
	Owner     sql.NullString  `db:"owner"`
	JobSet    sql.NullString  `db:"jobset"`
	Priority  sql.NullFloat64 `db:"priority"`
	Submitted pq.NullTime     `db:"submitted"`
	Cancelled pq.NullTime     `db:"cancelled"`
	JobJson   sql.NullString  `db:"job"`
	RunId     sql.NullString  `db:"run_id"`
	Cluster   sql.NullString  `db:"cluster"`
	Node      sql.NullString  `db:"node"`
	Created   pq.NullTime     `db:"created"`
	Started   pq.NullTime     `db:"started"`
	Finished  pq.NullTime     `db:"finished"`
	Succeeded sql.NullBool    `db:"succeeded"`
	Error     sql.NullString  `db:"error"`
}

var JobStates = &jobStates{
	Queued:    "QUEUED",
	Pending:   "PENDING",
	Running:   "RUNNING",
	Succeeded: "SUCCEEDED",
	Failed:    "FAILED",
	Cancelled: "CANCELLED",
}

var AllJobStates = []string{
	JobStates.Queued,
	JobStates.Pending,
	JobStates.Running,
	JobStates.Succeeded,
	JobStates.Failed,
	JobStates.Cancelled,
}

var FiltersForState = map[string][]goqu.Expression{
	JobStates.Queued: {
		job_submitted.IsNotNull(),
		job_cancelled.IsNull(),
		jobRun_created.IsNull(),
		jobRun_started.IsNull(),
		jobRun_finished.IsNull(),
		jobRun_unableToSchedule.IsNull(),
	},
	JobStates.Pending: {
		job_cancelled.IsNull(),
		jobRun_created.IsNotNull(),
		jobRun_started.IsNull(),
		jobRun_finished.IsNull(),
		jobRun_unableToSchedule.IsNull(),
	},
	JobStates.Running: {
		job_cancelled.IsNull(),
		jobRun_started.IsNotNull(),
		jobRun_finished.IsNull(),
		jobRun_unableToSchedule.IsNull(),
	},
	JobStates.Succeeded: {
		job_cancelled.IsNull(),
		jobRun_finished.IsNotNull(),
		jobRun_succeeded.IsTrue(),
		jobRun_unableToSchedule.IsNull(),
	},
	JobStates.Failed: {
		jobRun_succeeded.IsFalse(),
	},
	JobStates.Cancelled: {
		job_cancelled.IsNotNull(),
	},
}

func NewSQLJobRepository(db *goqu.Database, clock Clock) *SQLJobRepository {
	return &SQLJobRepository{goquDb: db, clock: clock}
}
