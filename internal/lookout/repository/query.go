package repository

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"

	"github.com/G-Research/armada/pkg/api"
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

type JobRepository interface {
	GetQueueStats(ctx context.Context) ([]*lookout.QueueInfo, error)
	GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error)
}

type SQLJobRepository struct {
	goquDb *goqu.Database
}

func NewSQLJobRepository(db *goqu.Database) *SQLJobRepository {
	return &SQLJobRepository{goquDb: db}
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
	jobRun_runId     = goqu.I("job_run.run_id")
	jobRun_jobId     = goqu.I("job_run.job_id")
	jobRun_cluster   = goqu.I("job_run.cluster")
	jobRun_node      = goqu.I("job_run.node")
	jobRun_created   = goqu.I("job_run.created")
	jobRun_started   = goqu.I("job_run.started")
	jobRun_finished  = goqu.I("job_run.finished")
	jobRun_succeeded = goqu.I("job_run.succeeded")
	jobRun_error     = goqu.I("job_run.error")
)

type queueStatsRow struct {
	Queue       string `db:"queue"`
	Jobs        uint32 `db:"jobs"`
	JobsCreated uint32 `db:"jobs_created"`
	JobsStarted uint32 `db:"jobs_started"`
}

func (r *SQLJobRepository) GetQueueStats(ctx context.Context) ([]*lookout.QueueInfo, error) {
	ds := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_queue,
			goqu.COUNT("*").As("jobs"),
			goqu.COUNT(goqu.COALESCE(jobRun_created, jobRun_started)).As("jobs_created"),
			goqu.COUNT(jobRun_started).As("jobs_started")).
		Where(jobRun_finished.IsNull()).
		GroupBy(job_queue)

	queueStatsRows := make([]*queueStatsRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &queueStatsRows)
	if err != nil {
		return nil, err
	}

	var result []*lookout.QueueInfo
	for _, row := range queueStatsRows {
		result = append(result, &lookout.QueueInfo{
			Queue:       row.Queue,
			JobsQueued:  row.Jobs - row.JobsCreated,
			JobsPending: row.JobsCreated - row.JobsStarted,
			JobsRunning: row.JobsStarted,
		})
	}
	return result, nil
}

func (r *SQLJobRepository) GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error) {
	if valid, jobState := validateJobStates(opts.JobStates); !valid {
		return nil, fmt.Errorf("unknown job state: %q", jobState)
	}

	rows, err := r.queryJobsInQueue(ctx, opts)
	if err != nil {
		return nil, err
	}

	result := jobsInQueueRowsToResult(rows)
	return result, nil
}

func validateJobStates(jobStates []string) (bool, string) {
	for _, jobState := range jobStates {
		if !isJobState(jobState) {
			return false, jobState
		}
	}
	return true, ""
}

func isJobState(val string) bool {
	for _, jobState := range AllJobStates {
		if val == jobState {
			return true
		}
	}
	return false
}

type jobsInQueueRow struct {
	JobId     string          `db:"job_id"`
	Queue     string          `db:"queue"`
	Owner     string          `db:"owner"`
	JobSet    string          `db:"jobset"`
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

func (r *SQLJobRepository) queryJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*jobsInQueueRow, error) {
	ds := r.createGetJobsInQueueDataset(opts)

	jobsInQueueRows := make([]*jobsInQueueRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobsInQueueRows)
	if err != nil {
		return nil, err
	}

	return jobsInQueueRows, nil
}

func (r *SQLJobRepository) createGetJobsInQueueDataset(opts *lookout.GetJobsInQueueRequest) *goqu.SelectDataset {
	subDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(job_jobId).
		Where(goqu.And(
			job_queue.Eq(opts.Queue),
			goqu.Or(createJobSetFilters(opts.JobSetIds)...))).
		GroupBy(job_jobId).
		Having(goqu.Or(createJobStateFilters(opts.JobStates)...)).
		Order(createJobOrdering(opts.NewestFirst)).
		Limit(uint(opts.Take)).
		Offset(uint(opts.Skip))

	ds := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(
			job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_queue,
			job_owner,
			job_jobset,
			job_priority,
			job_submitted,
			job_cancelled,
			job_job,
			jobRun_runId,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished,
			jobRun_succeeded,
			jobRun_error).
		Where(job_jobId.In(subDs)).
		Order(createJobOrdering(opts.NewestFirst)) // Ordering from sub query not guaranteed to be preserved

	return ds
}

func createJobSetFilters(jobSetIds []string) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, jobSetId := range jobSetIds {
		filter := job_jobset.Like(jobSetId + "%")
		filters = append(filters, filter)
	}
	return filters
}

var filtersForState = map[string][]goqu.Expression{
	JobStates.Queued: {
		goqu.MAX(job_submitted).IsNotNull(),
		goqu.MAX(job_cancelled).IsNull(),
		goqu.MAX(jobRun_created).IsNull(),
		goqu.MAX(jobRun_started).IsNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Pending: {
		goqu.MAX(job_cancelled).IsNull(),
		goqu.MAX(jobRun_created).IsNotNull(),
		goqu.MAX(jobRun_started).IsNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Running: {
		goqu.MAX(job_cancelled).IsNull(),
		goqu.MAX(jobRun_started).IsNotNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Succeeded: {
		goqu.MAX(job_cancelled).IsNull(),
		goqu.MAX(jobRun_finished).IsNotNull(),
		BOOL_OR(jobRun_succeeded).IsTrue(),
	},
	JobStates.Failed: {
		BOOL_OR(jobRun_succeeded).IsFalse(),
	},
	JobStates.Cancelled: {
		goqu.MAX(job_cancelled).IsNotNull(),
	},
}

func createJobStateFilters(jobStates []string) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, state := range jobStates {
		filter := goqu.And(filtersForState[state]...)
		filters = append(filters, filter)
	}
	return filters
}

func createJobOrdering(newestFirst bool) exp.OrderedExpression {
	if newestFirst {
		return job_jobId.Desc()
	}
	return job_jobId.Asc()
}

func jobsInQueueRowsToResult(rows []*jobsInQueueRow) []*lookout.JobInfo {
	result := make([]*lookout.JobInfo, 0)

	for i, row := range rows {
		if i == 0 || result[len(result)-1].Job.Id != row.JobId {
			result = append(result, &lookout.JobInfo{
				Job: &api.Job{
					Id:          row.JobId,
					JobSetId:    row.JobSet,
					Queue:       row.Queue,
					Namespace:   "",
					Labels:      nil,
					Annotations: nil,
					Owner:       row.Owner,
					Priority:    ParseNullFloat(row.Priority),
					PodSpec:     nil,
					Created:     ParseNullTimeDefault(row.Submitted), // Job submitted
				},
				Cancelled: ParseNullTime(row.Cancelled),
				JobState:  "",
				Runs:      []*lookout.RunInfo{},
			})
		}

		if row.RunId.Valid {
			result[len(result)-1].Runs = append(result[len(result)-1].Runs, &lookout.RunInfo{
				K8SId:     ParseNullString(row.RunId),
				Cluster:   ParseNullString(row.Cluster),
				Node:      ParseNullString(row.Node),
				Succeeded: ParseNullBool(row.Succeeded),
				Error:     ParseNullString(row.Error),
				Created:   ParseNullTime(row.Created), // Pod created (Pending)
				Started:   ParseNullTime(row.Started), // Pod running
				Finished:  ParseNullTime(row.Finished),
			})
		}
	}

	for i, jobInfo := range result {
		jobState := determineJobState(jobInfo)
		result[i].JobState = jobState
	}

	return result
}

func determineJobState(jobInfo *lookout.JobInfo) string {
	if jobInfo.Cancelled != nil {
		return JobStates.Cancelled
	}
	if len(jobInfo.Runs) > 0 {
		lastRun := jobInfo.Runs[len(jobInfo.Runs)-1]
		if lastRun.Finished != nil && lastRun.Succeeded {
			return JobStates.Succeeded
		}
		if lastRun.Finished != nil && !lastRun.Succeeded {
			return JobStates.Failed
		}
		if lastRun.Started != nil {
			return JobStates.Running
		}
		if lastRun.Created != nil {
			return JobStates.Pending
		}
	}
	return JobStates.Queued
}
