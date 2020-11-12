package repository

import (
	"database/sql"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	"github.com/doug-martin/goqu/v9/exp"
	"github.com/lib/pq"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/lookout"
)

type JobRepository interface {
	GetQueueStats() ([]*lookout.QueueInfo, error)
	GetJobsInQueue(opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error)
}

type SQLJobRepository struct {
	db *sql.DB
}

func NewSQLJobRepository(db *sql.DB) *SQLJobRepository {
	return &SQLJobRepository{db: db}
}

func (r *SQLJobRepository) GetQueueStats() ([]*lookout.QueueInfo, error) {
	rows, err := r.db.Query(`
		SELECT job.queue as queue, 
		       count(*) as jobs,
		       count(coalesce(job_run.created, job_run.started)) as jobs_created,
			   count(job_run.started) as Jobs_started
		FROM job LEFT JOIN job_run ON job.job_id = job_run.job_id
		WHERE job_run.finished IS NULL
		GROUP BY job.queue`)
	if err != nil {
		return nil, err
	}
	var (
		queue                          string
		jobs, jobsCreated, jobsStarted uint32
	)

	result := []*lookout.QueueInfo{}
	for rows.Next() {
		err := rows.Scan(&queue, &jobs, &jobsCreated, &jobsStarted)
		if err != nil {
			return nil, err
		}
		result = append(result, &lookout.QueueInfo{
			Queue:       queue,
			JobsQueued:  jobs - jobsCreated,
			JobsPending: jobsCreated - jobsStarted,
			JobsRunning: jobsStarted,
		})
	}
	return result, nil
}

func (r *SQLJobRepository) GetJobsInQueue(opts *lookout.GetJobsInQueueRequest) ([]*lookout.JobInfo, error) {
	rows, err := r.queryJobsInQueue(opts)
	if err != nil {
		return nil, err
	}

	result := parseJobsInQueueRows(rows)
	return result, nil
}

type jobsInQueueRow struct {
	JobId     string
	Queue     string
	Owner     string
	JobSet    string
	Priority  sql.NullFloat64
	Submitted pq.NullTime
	Cancelled pq.NullTime
	JobJson   sql.NullString
	RunId     sql.NullString
	Cluster   sql.NullString
	Node      sql.NullString
	Created   pq.NullTime
	Started   pq.NullTime
	Finished  pq.NullTime
	Succeeded sql.NullBool
	Error     sql.NullString
}

func (r *SQLJobRepository) queryJobsInQueue(opts *lookout.GetJobsInQueueRequest) ([]*jobsInQueueRow, error) {
	query, args, err := makeGetJobsInQueueQuery(opts)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	joinedRows := make([]*jobsInQueueRow, 0)
	for rows.Next() {
		row := &jobsInQueueRow{}
		err := rows.Scan(
			&row.JobId,
			&row.Owner,
			&row.JobSet,
			&row.Priority,
			&row.Submitted,
			&row.Cancelled,
			&row.JobJson,
			&row.RunId,
			&row.Cluster,
			&row.Node,
			&row.Created,
			&row.Started,
			&row.Finished,
			&row.Succeeded,
			&row.Error,
		)
		if err != nil {
			return nil, err
		}
		joinedRows = append(joinedRows, row)
	}
	return joinedRows, nil
}

func makeGetJobsInQueueQuery(opts *lookout.GetJobsInQueueRequest) (string, []interface{}, error) {
	dialect := goqu.Dialect("postgres")

	subDs := dialect.
		From("job").
		LeftJoin(goqu.T("job_run"), goqu.On(goqu.Ex{
			"job.job_id": goqu.I("job_run.job_id"),
		})).
		Select(goqu.I("job.job_id")).
		Where(goqu.And(
			goqu.I("job.queue").Eq(opts.Queue),
			goqu.Or(createJobSetFilters(opts.JobSetIds)...))).
		GroupBy(goqu.I("job.job_id")).
		Having(goqu.Or(createJobStateFilters(opts.JobStates)...)).
		Order(createJobOrdering(opts.NewestFirst)).
		Limit(uint(opts.Take)).
		Offset(uint(opts.Skip))

	ds := dialect.
		From("job").
		LeftJoin(goqu.T("job_run"), goqu.On(goqu.Ex{
			"job.job_id": goqu.I("job_run.job_id"),
		})).
		Prepared(true).
		Select(
			goqu.I("job.job_id"),
			goqu.I("job.owner"),
			goqu.I("job.jobset"),
			goqu.I("job.priority"),
			goqu.I("job.submitted"),
			goqu.I("job.cancelled"),
			goqu.I("job.job"),
			goqu.I("job_run.run_id"),
			goqu.I("job_run.cluster"),
			goqu.I("job_run.node"),
			goqu.I("job_run.created"),
			goqu.I("job_run.started"),
			goqu.I("job_run.finished"),
			goqu.I("job_run.succeeded"),
			goqu.I("job_run.error")).
		Where(goqu.I("job.job_id").In(subDs)).
		Order(createJobOrdering(opts.NewestFirst)) // Ordering from sub query not guaranteed to be preserved

	return ds.ToSQL()
}

func createJobSetFilters(jobSetIds []string) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, jobSetId := range jobSetIds {
		filter := goqu.I("job.jobset").Like(jobSetId + "%")
		filters = append(filters, filter)
	}
	return filters
}

const (
	submitted = "job.submitted"
	cancelled = "job.cancelled"
	created   = "job_run.created"
	started   = "job_run.started"
	finished  = "job_run.finished"
	succeeded = "job_run.succeeded"
)

var filtersForState = map[lookout.JobState][]goqu.Expression{
	lookout.JobState_QUEUED: {
		goqu.MAX(goqu.I(submitted)).IsNotNull(),
		goqu.MAX(goqu.I(cancelled)).IsNull(),
		goqu.MAX(goqu.I(created)).IsNull(),
		goqu.MAX(goqu.I(started)).IsNull(),
		goqu.MAX(goqu.I(finished)).IsNull(),
	},
	lookout.JobState_PENDING: {
		goqu.MAX(goqu.I(cancelled)).IsNull(),
		goqu.MAX(goqu.I(created)).IsNotNull(),
		goqu.MAX(goqu.I(started)).IsNull(),
		goqu.MAX(goqu.I(finished)).IsNull(),
	},
	lookout.JobState_RUNNING: {
		goqu.MAX(goqu.I(cancelled)).IsNull(),
		goqu.MAX(goqu.I(started)).IsNotNull(),
		goqu.MAX(goqu.I(finished)).IsNull(),
	},
	lookout.JobState_SUCCEEDED: {
		goqu.MAX(goqu.I(cancelled)).IsNull(),
		goqu.MAX(goqu.I(finished)).IsNotNull(),
		BOOL_OR(goqu.I(succeeded)).IsTrue(),
	},
	lookout.JobState_FAILED: {
		BOOL_OR(goqu.I(succeeded)).IsFalse(),
	},
	lookout.JobState_CANCELLED: {
		goqu.MAX(goqu.I(cancelled)).IsNotNull(),
	},
}

func createJobStateFilters(jobStates []lookout.JobState) []goqu.Expression {
	filters := make([]goqu.Expression, 0)
	for _, state := range jobStates {
		filter := goqu.And(filtersForState[state]...)
		filters = append(filters, filter)
	}
	return filters
}

func BOOL_OR(col interface{}) exp.SQLFunctionExpression {
	return goqu.Func("BOOL_OR", col)
}

func createJobOrdering(newestFirst bool) exp.OrderedExpression {
	jobId := goqu.I("job.job_id")
	if newestFirst {
		return jobId.Desc()
	}
	return jobId.Asc()
}

func parseJobsInQueueRows(rows []*jobsInQueueRow) []*lookout.JobInfo {
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
					Priority:    parseNullFloat(row.Priority),
					PodSpec:     nil,
					Created:     parseNullTimeDefault(row.Submitted), // Job submitted
				},
				Cancelled: parseNullTime(row.Cancelled),
				Runs:      []*lookout.RunInfo{},
			})
		}

		if row.RunId.Valid {
			result[len(result)-1].Runs = append(result[len(result)-1].Runs, &lookout.RunInfo{
				K8SId:     parseNullString(row.RunId),
				Cluster:   parseNullString(row.Cluster),
				Node:      parseNullString(row.Node),
				Succeeded: parseNullBool(row.Succeeded),
				Error:     parseNullString(row.Error),
				Created:   parseNullTime(row.Created), // Pod created (Pending)
				Started:   parseNullTime(row.Started), // Pod running
				Finished:  parseNullTime(row.Finished),
			})
		}
	}
	return result
}

func parseNullString(nullString sql.NullString) string {
	if !nullString.Valid {
		return ""
	}
	return nullString.String
}

func parseNullBool(nullBool sql.NullBool) bool {
	if !nullBool.Valid {
		return false
	}
	return nullBool.Bool
}

func parseNullFloat(nullFloat sql.NullFloat64) float64 {
	if !nullFloat.Valid {
		return 0
	}
	return nullFloat.Float64
}

func parseNullTime(nullTime pq.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func parseNullTimeDefault(nullTime pq.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Time{}
	}
	return nullTime.Time
}
