package repository

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"

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

type JobRepository interface {
	GetQueueInfos(ctx context.Context) ([]*lookout.QueueInfo, error)
	GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error)
}

type SQLJobRepository struct {
	goquDb *goqu.Database
}

type queueInfoRow struct {
	Queue       string `db:"queue"`
	Jobs        uint32 `db:"jobs"`
	JobsCreated uint32 `db:"jobs_created"`
	JobsStarted uint32 `db:"jobs_started"`

	OldestQueuedJobId     sql.NullString  `db:"oldest_queued_job_id"`
	OldestQueuedJobSet    sql.NullString  `db:"oldest_queued_jobset"`
	OldestQueuedOwner     sql.NullString  `db:"oldest_queued_owner"`
	OldestQueuedPriority  sql.NullFloat64 `db:"oldest_queued_priority"`
	OldestQueuedSubmitted pq.NullTime     `db:"oldest_queued_submitted"`

	LongestRunningJobId     sql.NullString  `db:"longest_running_job_id"`
	LongestRunningJobSet    sql.NullString  `db:"longest_running_jobset"`
	LongestRunningOwner     sql.NullString  `db:"longest_running_owner"`
	LongestRunningPriority  sql.NullFloat64 `db:"longest_running_priority"`
	LongestRunningSubmitted pq.NullTime     `db:"longest_running_submitted"`
	LongestRunningRunId     sql.NullString  `db:"longest_running_run_id"`
	LongestRunningCluster   sql.NullString  `db:"longest_running_cluster"`
	LongestRunningNode      sql.NullString  `db:"longest_running_node"`
	LongestRunningCreated   pq.NullTime     `db:"longest_running_created"`
	LongestRunningStarted   pq.NullTime     `db:"longest_running_started"`
}

type countsRow struct {
	Jobs        uint32 `db:"jobs"`
	JobsCreated uint32 `db:"jobs_created"`
	JobsStarted uint32 `db:"jobs_started"`
}

type jobRow struct {
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

var filtersForState = map[string][]goqu.Expression{
	JobStates.Queued: {
		job_submitted.IsNotNull(),
		job_cancelled.IsNull(),
		goqu.MAX(jobRun_created).IsNull(),
		goqu.MAX(jobRun_started).IsNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Pending: {
		job_cancelled.IsNull(),
		goqu.MAX(jobRun_created).IsNotNull(),
		goqu.MAX(jobRun_started).IsNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Running: {
		job_cancelled.IsNull(),
		goqu.MAX(jobRun_started).IsNotNull(),
		goqu.MAX(jobRun_finished).IsNull(),
	},
	JobStates.Succeeded: {
		job_cancelled.IsNull(),
		goqu.MAX(jobRun_finished).IsNotNull(),
		BOOL_OR(jobRun_succeeded).IsTrue(),
	},
	JobStates.Failed: {
		BOOL_OR(jobRun_succeeded).IsFalse(),
	},
	JobStates.Cancelled: {
		job_cancelled.IsNotNull(),
	},
}

func NewSQLJobRepository(db *goqu.Database) *SQLJobRepository {
	return &SQLJobRepository{goquDb: db}
}

func (r *SQLJobRepository) GetQueueInfos(ctx context.Context) ([]*lookout.QueueInfo, error) {
	var err error

	countsSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_queue,
			goqu.MAX(jobRun_created).As("created"),
			goqu.MAX(jobRun_started).As("started")).
		GroupBy(job_jobId).
		Having(goqu.And(goqu.MAX(jobRun_finished).IsNull(), job_cancelled.IsNull())).
		As("counts_sub") // Identify unique created and started jobs

	countsDs := r.goquDb.
		From(countsSubDs).
		Select(
			goqu.I("counts_sub.queue"),
			goqu.COUNT("*").As("jobs"),
			goqu.COUNT(
				goqu.COALESCE(
					goqu.I("counts_sub.created"),
					goqu.I("counts_sub.started"))).As("jobs_created"),
			goqu.COUNT(goqu.I("counts_sub.started")).As("jobs_started")).
		GroupBy(goqu.I("counts_sub.queue")).
		As("counts")

	countsSql, _, err := countsDs.ToSQL()

	oldestQueuedDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_jobset,
			job_queue,
			job_owner,
			job_priority,
			job_submitted,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		Distinct(job_queue).
		Where(goqu.And(
			job_submitted.IsNotNull(),
			job_cancelled.IsNull(),
			jobRun_created.IsNull(),
			jobRun_started.IsNull(),
			jobRun_finished.IsNull())).
		Order(job_queue.Asc(), job_submitted.Asc()).
		As("oldest_queued")

	oldestQueuedSql, _, err := oldestQueuedDs.ToSQL()

	longestRunningSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_jobId,
			job_jobset,
			job_queue,
			job_owner,
			job_priority,
			job_submitted,
			goqu.MAX(jobRun_started).As("started")).
		Distinct(job_queue).
		GroupBy(job_jobId).
		Having(goqu.And(filtersForState[JobStates.Running]...)).
		Order(job_queue.Asc(), goqu.I("started").Asc()).
		As("longest_running_sub") // Identify longest running jobs

	longestRunningDs := r.goquDb.
		From(longestRunningSubDs).
		LeftJoin(jobRunTable, goqu.On(goqu.I("longest_running_sub.job_id").Eq(jobRun_jobId))).
		Select(
			goqu.I("longest_running_sub.job_id"),
			goqu.I("longest_running_sub.jobset"),
			goqu.I("longest_running_sub.queue"),
			goqu.I("longest_running_sub.owner"),
			goqu.I("longest_running_sub.priority"),
			goqu.I("longest_running_sub.submitted"),
			jobRun_runId,
			jobRun_cluster,
			jobRun_node,
			jobRun_created,
			jobRun_started,
			jobRun_finished).
		As("longest_running")

	longestRunningSql, _, err := longestRunningDs.ToSQL()

	r.goquDb.
		From(countsDs).
		LeftJoin(longestRunningDs, goqu.On(goqu.I("counts.queue").Eq(goqu.I("longest_running.queue")))).
		LeftJoin(oldestQueuedDs, goqu.On(goqu.I("counts.queue").Eq(goqu.I("oldest_queued.queue")))).
		Select(
			goqu.I("counts.queue"),
			goqu.I("counts.jobs"),
			goqu.I("counts.jobs_created"),
			goqu.I("counts.jobs_started"),
			goqu.I("oldest_queued.job_id").As("oldest_queued_job_id"),
			goqu.I("oldest_queued.jobset").As("oldest_queued_jobset"),
			goqu.I("oldest_queued.owner").As("oldest_queued_owner"),
			goqu.I("oldest_queued.priority").As("oldest_queued_priority"),
			goqu.I("oldest_queued.submitted").As("oldest_queued_submitted"),
			goqu.I("longest_running.job_id").As("longest_running_job_id"),
			goqu.I("longest_running.jobset").As("longest_running_jobset"),
			goqu.I("longest_running.owner").As("longest_running_owner"),
			goqu.I("longest_running.priority").As("longest_running_priority"),
			goqu.I("longest_running.submitted").As("longest_running_submitted"),
			goqu.I("longest_running.run_id").As("longest_running_run_id"),
			goqu.I("longest_running.cluster").As("longest_running_cluster"),
			goqu.I("longest_running.node").As("longest_running_node"),
			goqu.I("longest_running.created").As("longest_running_created"),
			goqu.I("longest_running.started").As("longest_running_started"))

	queries := strings.Join([]string{countsSql, oldestQueuedSql, longestRunningSql}, " ; ")
	rows, err := r.goquDb.Db.QueryContext(ctx, queries)
	if err != nil {
		return nil, err
	}
	defer func() {
		err := rows.Close()
		if err != nil {
			log.Fatalf("failed to close SQL connection: %v", err)
		}
	}()

	resultMap := make(map[string]*lookout.QueueInfo)
	// Counts
	for rows.Next() {
		var (
			queue string
			row   countsRow
		)
		err = rows.Scan(&queue, &row.Jobs, &row.JobsCreated, &row.JobsStarted)
		if err != nil {
			return nil, err
		}
		resultMap[queue] = &lookout.QueueInfo{
			Queue:             queue,
			JobsQueued:        row.Jobs - row.JobsCreated,
			JobsPending:       row.JobsCreated - row.JobsStarted,
			JobsRunning:       row.JobsStarted,
			OldestQueuedJob:   nil,
			LongestRunningJob: nil,
		}
	}

	// Oldest queued
	if rows.NextResultSet() {
		for rows.Next() {
			var row jobRow
			err := rows.Scan(
				&row.JobId,
				&row.JobSet,
				&row.Queue,
				&row.Owner,
				&row.Priority,
				&row.Submitted,
				&row.Created,
				&row.Started,
				&row.Finished)
			if err != nil {
				return nil, err
			}
			if row.Queue.Valid {
				resultMap[row.Queue.String].OldestQueuedJob = &lookout.JobInfo{
					Job: &api.Job{
						Id:          ParseNullString(row.JobId),
						JobSetId:    ParseNullString(row.JobSet),
						Queue:       ParseNullString(row.Queue),
						Namespace:   "",
						Labels:      nil,
						Annotations: nil,
						Owner:       ParseNullString(row.Owner),
						Priority:    ParseNullFloat(row.Priority),
						PodSpec:     nil,
						Created:     ParseNullTimeDefault(row.Submitted),
					},
					Runs:      []*lookout.RunInfo{},
					Cancelled: nil,
					JobState:  JobStates.Queued,
				}
			}
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for oldest queued job: %v", rows.Err())
	}

	// Longest running
	if rows.NextResultSet() {
		for rows.Next() {
			var row jobRow
			err := rows.Scan(
				&row.JobId,
				&row.JobSet,
				&row.Queue,
				&row.Owner,
				&row.Priority,
				&row.Submitted,
				&row.RunId,
				&row.Cluster,
				&row.Node,
				&row.Created,
				&row.Started,
				&row.Finished)
			if err != nil {
				return nil, err
			}
			if row.Queue.Valid {
				queueInfo := resultMap[row.Queue.String]
				if queueInfo.LongestRunningJob != nil {
					queueInfo.LongestRunningJob.Runs = append(queueInfo.LongestRunningJob.Runs, &lookout.RunInfo{
						K8SId:     ParseNullString(row.RunId),
						Cluster:   ParseNullString(row.Cluster),
						Node:      ParseNullString(row.Node),
						Succeeded: false,
						Error:     "",
						Created:   ParseNullTime(row.Created),
						Started:   ParseNullTime(row.Started),
						Finished:  nil,
					})
				} else {
					queueInfo.LongestRunningJob = &lookout.JobInfo{
						Job: &api.Job{
							Id:          ParseNullString(row.JobId),
							JobSetId:    ParseNullString(row.JobSet),
							Queue:       ParseNullString(row.Queue),
							Namespace:   "",
							Labels:      nil,
							Annotations: nil,
							Owner:       ParseNullString(row.Owner),
							Priority:    ParseNullFloat(row.Priority),
							PodSpec:     nil,
							Created:     ParseNullTimeDefault(row.Submitted),
						},
						Runs: []*lookout.RunInfo{
							{
								K8SId:     ParseNullString(row.RunId),
								Cluster:   ParseNullString(row.Cluster),
								Node:      ParseNullString(row.Node),
								Succeeded: false,
								Error:     "",
								Created:   ParseNullTime(row.Created),
								Started:   ParseNullTime(row.Started),
								Finished:  nil,
							},
						},
						Cancelled: nil,
						JobState:  JobStates.Running,
					}
				}
			}
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for longest running job: %v", rows.Err())
	}

	var queues []string
	for queue := range resultMap {
		queues = append(queues, queue)
	}
	sort.Strings(queues)

	var result []*lookout.QueueInfo
	for _, queue := range queues {
		result = append(result, resultMap[queue])
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

	return jobsInQueueRowsToResult(rows), nil
}

func (r *SQLJobRepository) queueInfoRowsToResult(queueInfoRows []*queueInfoRow) []*lookout.QueueInfo {
	queueInfoMap := make(map[string]*lookout.QueueInfo)
	for _, row := range queueInfoRows {
		if queueInfo, ok := queueInfoMap[row.Queue]; ok {
			if row.LongestRunningJobId.Valid {
				queueInfo.LongestRunningJob.Runs = append(queueInfo.LongestRunningJob.Runs, makeLongestRunningJobRun(row))
			}
		} else {
			var oldestQueuedJob *lookout.JobInfo
			if row.OldestQueuedJobId.Valid {
				oldestQueuedJob = makeOldestQueuedJob(row)
			}

			var longestRunningJob *lookout.JobInfo
			if row.LongestRunningJobId.Valid {
				longestRunningJob = makeLongestRunningJob(row)
			}

			queueInfoMap[row.Queue] = &lookout.QueueInfo{
				Queue:             row.Queue,
				JobsQueued:        row.Jobs - row.JobsCreated,
				JobsPending:       row.JobsCreated - row.JobsStarted,
				JobsRunning:       row.JobsStarted,
				OldestQueuedJob:   oldestQueuedJob,
				LongestRunningJob: longestRunningJob,
			}
		}
	}

	var queues []string
	for queue := range queueInfoMap {
		queues = append(queues, queue)
	}
	sort.Strings(queues)

	var result []*lookout.QueueInfo
	for _, queue := range queues {
		result = append(result, queueInfoMap[queue])
	}
	return result
}

func makeOldestQueuedJob(row *queueInfoRow) *lookout.JobInfo {
	return &lookout.JobInfo{
		Job: &api.Job{
			Id:          ParseNullString(row.OldestQueuedJobId),
			JobSetId:    ParseNullString(row.OldestQueuedJobSet),
			Queue:       row.Queue,
			Namespace:   "",
			Labels:      nil,
			Annotations: nil,
			Owner:       ParseNullString(row.OldestQueuedOwner),
			Priority:    ParseNullFloat(row.OldestQueuedPriority),
			PodSpec:     nil,
			Created:     ParseNullTimeDefault(row.OldestQueuedSubmitted),
		},
		Runs:      []*lookout.RunInfo{},
		Cancelled: nil,
		JobState:  JobStates.Queued,
	}
}

func makeLongestRunningJob(row *queueInfoRow) *lookout.JobInfo {
	return &lookout.JobInfo{
		Job: &api.Job{
			Id:          ParseNullString(row.LongestRunningJobId),
			JobSetId:    ParseNullString(row.LongestRunningJobSet),
			Queue:       row.Queue,
			Namespace:   "",
			Labels:      nil,
			Annotations: nil,
			Owner:       ParseNullString(row.LongestRunningOwner),
			Priority:    ParseNullFloat(row.LongestRunningPriority),
			PodSpec:     nil,
			Created:     ParseNullTimeDefault(row.LongestRunningSubmitted),
		},
		Runs:      []*lookout.RunInfo{makeLongestRunningJobRun(row)},
		Cancelled: nil,
		JobState:  JobStates.Running,
	}
}

func makeLongestRunningJobRun(row *queueInfoRow) *lookout.RunInfo {
	return &lookout.RunInfo{
		K8SId:     ParseNullString(row.LongestRunningRunId),
		Cluster:   ParseNullString(row.LongestRunningCluster),
		Node:      ParseNullString(row.LongestRunningNode),
		Succeeded: false,
		Error:     "",
		Created:   ParseNullTime(row.LongestRunningCreated),
		Started:   ParseNullTime(row.LongestRunningStarted),
		Finished:  nil,
	}
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

func (r *SQLJobRepository) queryJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*jobRow, error) {
	ds := r.createGetJobsInQueueDataset(opts)

	jobsInQueueRows := make([]*jobRow, 0)
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

func jobsInQueueRowsToResult(rows []*jobRow) []*lookout.JobInfo {
	result := make([]*lookout.JobInfo, 0)

	for i, row := range rows {
		if row.JobId.Valid &&
			(i == 0 || result[len(result)-1].Job.Id != row.JobId.String) {
			result = append(result, &lookout.JobInfo{
				Job: &api.Job{
					Id:          row.JobId.String,
					JobSetId:    ParseNullString(row.JobSet),
					Queue:       ParseNullString(row.Queue),
					Namespace:   "",
					Labels:      nil,
					Annotations: nil,
					Owner:       ParseNullString(row.Owner),
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
