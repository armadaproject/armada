package repository

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"sort"
	"strings"

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
	GetJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error)
	GetJobsInQueue(ctx context.Context, opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error)
}

type SQLJobRepository struct {
	goquDb *goqu.Database
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

type jobSetCountsRow struct {
	JobSet        string        `db:"jobset"`
	Jobs          sql.NullInt32 `db:"jobs"`
	JobsCreated   sql.NullInt32 `db:"jobs_created"`
	JobsStarted   sql.NullInt32 `db:"jobs_started"`
	JobsFinished  sql.NullInt32 `db:"jobs_finished"`
	JobsSucceeded sql.NullInt32 `db:"jobs_succeeded"`
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
	queries, err := r.getQueueInfosSql()
	if err != nil {
		return nil, err
	}

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

	queueInfoMap := make(map[string]*lookout.QueueInfo)

	// Job counts
	err = setJobCountsForQueueInfos(rows, queueInfoMap)
	if err != nil {
		return nil, err
	}

	// Oldest queued
	if rows.NextResultSet() {
		err = setOldestQueuedJobForQueueInfo(rows, queueInfoMap)
		if err != nil {
			return nil, err
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for oldest queued job: %v", rows.Err())
	}

	// Longest running
	if rows.NextResultSet() {
		err = setLongestRunningJobForQueueInfos(rows, queueInfoMap)
		if err != nil {
			return nil, err
		}
	} else if rows.Err() != nil {
		return nil, fmt.Errorf("expected result set for longest running job: %v", rows.Err())
	}

	result := getSortedQueueInfos(queueInfoMap)

	return result, nil
}

func (r *SQLJobRepository) GetJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*lookout.JobSetInfo, error) {
	rows, err := r.queryJobSetInfos(ctx, opts)
	if err != nil {
		return nil, err
	}

	return jobSetInfoRowsToResult(rows, opts.Queue), nil
}

func jobSetInfoRowsToResult(rows []*jobSetCountsRow, queue string) []*lookout.JobSetInfo {
	jobSetInfoMap := make(map[string]*lookout.JobSetInfo)

	for _, row := range rows {
		jobs := uint32(ParseNullInt(row.Jobs))
		jobsCreated := uint32(ParseNullInt(row.JobsCreated))
		jobsStarted := uint32(ParseNullInt(row.JobsStarted))
		jobsSucceeded := uint32(ParseNullInt(row.JobsSucceeded))
		jobsFinished := uint32(ParseNullInt(row.JobsFinished))
		jobSetInfoMap[row.JobSet] = &lookout.JobSetInfo{
			Queue:         queue,
			JobSet:        row.JobSet,
			JobsQueued:    jobs - jobsCreated,
			JobsPending:   jobsCreated - jobsStarted,
			JobsRunning:   jobsStarted,
			JobsSucceeded: jobsSucceeded,
			JobsFailed:    jobsFinished - jobsSucceeded,
		}
	}

	return getSortedJobSetInfos(jobSetInfoMap)
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

func (r *SQLJobRepository) getQueueInfosSql() (string, error) {
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

	countsSql, _, err := countsDs.ToSQL()
	if err != nil {
		return "", err
	}
	oldestQueuedSql, _, err := oldestQueuedDs.ToSQL()
	if err != nil {
		return "", err
	}
	longestRunningSql, _, err := longestRunningDs.ToSQL()
	if err != nil {
		return "", err
	}

	// Execute three unprepared statements sequentially.
	// There are no parameters and we don't care if updates happen between queries.
	return strings.Join([]string{countsSql, oldestQueuedSql, longestRunningSql}, " ; "), nil
}

func setJobCountsForQueueInfos(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
	for rows.Next() {
		var (
			queue string
			row   countsRow
		)
		err := rows.Scan(&queue, &row.Jobs, &row.JobsCreated, &row.JobsStarted)
		if err != nil {
			return err
		}
		queueInfoMap[queue] = &lookout.QueueInfo{
			Queue:             queue,
			JobsQueued:        row.Jobs - row.JobsCreated,
			JobsPending:       row.JobsCreated - row.JobsStarted,
			JobsRunning:       row.JobsStarted,
			OldestQueuedJob:   nil,
			LongestRunningJob: nil,
		}
	}
	return nil
}

func setOldestQueuedJobForQueueInfo(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
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
			return err
		}
		if row.Queue.Valid {
			if queueInfo, ok := queueInfoMap[row.Queue.String]; queueInfo != nil && ok {
				queueInfo.OldestQueuedJob = &lookout.JobInfo{
					Job:       makeJobFromRow(&row),
					Runs:      []*lookout.RunInfo{},
					Cancelled: nil,
					JobState:  JobStates.Queued,
				}
			}
		}
	}
	return nil
}

func setLongestRunningJobForQueueInfos(rows *sql.Rows, queueInfoMap map[string]*lookout.QueueInfo) error {
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
			return err
		}
		if row.Queue.Valid {
			if queueInfo, ok := queueInfoMap[row.Queue.String]; queueInfo != nil && ok {
				if queueInfo.LongestRunningJob != nil {
					queueInfo.LongestRunningJob.Runs = append(queueInfo.LongestRunningJob.Runs, makeRunInfoFromRow(&row))
				} else {
					queueInfo.LongestRunningJob = &lookout.JobInfo{
						Job:       makeJobFromRow(&row),
						Runs:      []*lookout.RunInfo{makeRunInfoFromRow(&row)},
						Cancelled: nil,
						JobState:  JobStates.Running,
					}
				}
			}
		}
	}
	return nil
}

func getSortedQueueInfos(resultMap map[string]*lookout.QueueInfo) []*lookout.QueueInfo {
	var queues []string
	for queue := range resultMap {
		queues = append(queues, queue)
	}
	sort.Strings(queues)

	var result []*lookout.QueueInfo
	for _, queue := range queues {
		result = append(result, resultMap[queue])
	}
	return result
}

func (r *SQLJobRepository) queryJobSetInfos(ctx context.Context, opts *lookout.GetJobSetsRequest) ([]*jobSetCountsRow, error) {
	ds := r.createJobSetInfosDataset(opts)

	jobsInQueueRows := make([]*jobSetCountsRow, 0)
	err := ds.Prepared(true).ScanStructsContext(ctx, &jobsInQueueRows)
	if err != nil {
		return nil, err
	}

	return jobsInQueueRows, nil
}

func (r *SQLJobRepository) createJobSetInfosDataset(opts *lookout.GetJobSetsRequest) *goqu.SelectDataset {
	countsSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_queue,
			job_jobset,
			goqu.MAX(jobRun_created).As("created"),
			goqu.MAX(jobRun_started).As("started")).
		Where(job_queue.Eq(opts.Queue)).
		GroupBy(job_jobId).
		Having(goqu.And(goqu.MAX(jobRun_finished).IsNull(), job_cancelled.IsNull())).
		As("counts_sub") // Identify unique created and started jobs

	countsDs := r.goquDb.
		From(countsSubDs).
		Select(
			goqu.I("counts_sub.jobset"),
			goqu.COUNT("*").As("jobs"),
			goqu.COUNT(
				goqu.COALESCE(
					goqu.I("counts_sub.created"),
					goqu.I("counts_sub.started"))).As("jobs_created"),
			goqu.COUNT(goqu.I("counts_sub.started")).As("jobs_started")).
		GroupBy(goqu.I("counts_sub.jobset")).
		As("counts")

	finishedCountsSubDs := r.goquDb.
		From(jobTable).
		LeftJoin(jobRunTable, goqu.On(job_jobId.Eq(jobRun_jobId))).
		Select(
			job_queue,
			job_jobset,
			goqu.MAX(jobRun_finished).As("finished"),
			BOOL_OR(jobRun_succeeded).As("succeeded")).
		Where(job_queue.Eq(opts.Queue)).
		GroupBy(job_jobId).
		Having(goqu.And(goqu.MAX(jobRun_finished).IsNotNull(), job_cancelled.IsNull())).
		As("finished_counts_sub") // Identify unique finished jobs

	finishedCountsDs := r.goquDb.
		From(finishedCountsSubDs).
		Select(
			goqu.I("finished_counts_sub.jobset"),
			goqu.COUNT(goqu.I("finished_counts_sub.finished")).As("jobs_finished"),
			goqu.SUM(goqu.L("finished_counts_sub.succeeded::int")).As("jobs_succeeded")).
		GroupBy(goqu.I("finished_counts_sub.jobset")).
		As("finished_counts")

	ds := r.goquDb.
		From(finishedCountsDs).
		LeftJoin(countsDs, goqu.On(goqu.I("finished_counts.jobset").Eq(goqu.I("counts.jobset")))).
		Select(
			goqu.I("finished_counts.jobset"),
			goqu.I("counts.jobs"),
			goqu.I("counts.jobs_created"),
			goqu.I("counts.jobs_started"),
			goqu.I("finished_counts.jobs_finished"),
			goqu.I("finished_counts.jobs_succeeded"))

	qq, _, _ := ds.ToSQL()
	fmt.Println(qq)

	return ds
}

func getSortedJobSetInfos(resultMap map[string]*lookout.JobSetInfo) []*lookout.JobSetInfo {
	var jobSets []string
	for jobSet := range resultMap {
		jobSets = append(jobSets, jobSet)
	}
	sort.Strings(jobSets)

	var result []*lookout.JobSetInfo
	for _, jobSet := range jobSets {
		result = append(result, resultMap[jobSet])
	}
	return result
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
				Job:       makeJobFromRow(row),
				Cancelled: ParseNullTime(row.Cancelled),
				JobState:  "",
				Runs:      []*lookout.RunInfo{},
			})
		}

		if row.RunId.Valid {
			result[len(result)-1].Runs = append(result[len(result)-1].Runs, makeRunInfoFromRow(row))
		}
	}

	for i, jobInfo := range result {
		jobState := determineJobState(jobInfo)
		result[i].JobState = jobState
	}

	return result
}

func makeJobFromRow(row *jobRow) *api.Job {
	if row == nil {
		return nil
	}
	return &api.Job{
		Id:       ParseNullString(row.JobId),
		JobSetId: ParseNullString(row.JobSet),
		Queue:    ParseNullString(row.Queue),
		Owner:    ParseNullString(row.Owner),
		Priority: ParseNullFloat(row.Priority),
		Created:  ParseNullTimeDefault(row.Submitted),
	}
}

func makeRunInfoFromRow(row *jobRow) *lookout.RunInfo {
	if row == nil {
		return nil
	}
	return &lookout.RunInfo{
		K8SId:     ParseNullString(row.RunId),
		Cluster:   ParseNullString(row.Cluster),
		Node:      ParseNullString(row.Node),
		Succeeded: ParseNullBool(row.Succeeded),
		Error:     ParseNullString(row.Error),
		Created:   ParseNullTime(row.Created), // Pod created (Pending)
		Started:   ParseNullTime(row.Started), // Pod running
		Finished:  ParseNullTime(row.Finished),
	}
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
