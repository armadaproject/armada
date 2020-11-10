package repository

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

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

type joinedRow struct {
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

func (r *SQLJobRepository) queryJobsInQueue(opts *lookout.GetJobsInQueueRequest) ([]*joinedRow, error) {
	queryString := makeGetJobsInQueueQuery(opts)
	rows, err := r.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	joinedRows := make([]*joinedRow, 0)
	for rows.Next() {
		row := &joinedRow{}
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

func makeGetJobsInQueueQuery(opts *lookout.GetJobsInQueueRequest) string {
	query := `
		SELECT job.job_id as job_id,
			   job.owner as owner,
			   job.jobset as jobset,
               job.priority as priority,
               job.submitted as submitted,
               job.cancelled as cancelled,
			   job.job as job_json,
			   job_run.run_id as run_id,
			   job_run.cluster as cluster,
			   job_run.node as node,
			   job_run.created as created,
			   job_run.started as started,
			   job_run.finished as finished,
			   job_run.succeeded as succeeded,
			   job_run.error as error
		FROM job LEFT JOIN job_run ON job.job_id = job_run.job_id
		WHERE job.job_id IN (%s)`

	sb := &strings.Builder{} // Builder for sub query

	sb.WriteString(`
		SELECT job.job_id
		FROM job LEFT JOIN job_run ON job.job_id = job_run.job_id
	`)

	addWhereClause(sb, opts.Queue, opts.JobSetIds)

	sb.WriteString("GROUP BY job.job_id\n")

	addHavingClause(sb, opts.JobStates)

	addOrderByClause(sb, opts.NewestFirst)

	sb.WriteString(fmt.Sprintf("LIMIT %d\n", opts.Take))

	if opts.Skip > 0 {
		sb.WriteString(fmt.Sprintf("OFFSET %d\n", opts.Skip))
	}

	return fmt.Sprintf(query, sb.String())
}

func addWhereClause(sb *strings.Builder, queue string, jobSetIds []string) {
	sb.WriteString(fmt.Sprintf("WHERE job.queue  = '%s'\n", queue))

	if len(jobSetIds) > 0 {
		sb.WriteString("AND (")

		conditions := make([]string, len(jobSetIds))
		for i, jobSetId := range jobSetIds {
			conditions[i] = fmt.Sprintf("job.jobset LIKE '%s%%'", jobSetId)
		}
		sb.WriteString(joinConditions(conditions, "OR", false))

		sb.WriteString(")\n")
	}
}

const (
	submitted = "job.submitted"
	cancelled = "job.cancelled"
	created   = "job_run.created"
	started   = "job_run.started"
	finished  = "job_run.finished"
	succeeded = "job_run.succeeded"
)

var stateFiltersMap = map[lookout.JobState]map[string]bool{
	lookout.JobState_QUEUED: {
		submitted: true,
		cancelled: false,
		created:   false,
		started:   false,
		finished:  false,
	},
	lookout.JobState_PENDING: {
		cancelled: false,
		created:   true,
		started:   false,
		finished:  false,
	},
	lookout.JobState_RUNNING: {
		cancelled: false,
		started:   true,
		finished:  false,
	},
	lookout.JobState_SUCCEEDED: {
		cancelled: false,
		finished:  true,
		succeeded: true,
	},
	lookout.JobState_FAILED: {
		succeeded: false,
	},
	lookout.JobState_CANCELLED: {
		cancelled: true,
	},
}

func addHavingClause(sb *strings.Builder, jobStates []lookout.JobState) {
	conditions := make([]string, 0)

	for _, state := range jobStates {
		stateConditions := make([]string, 0)

		for column, include := range stateFiltersMap[state] {
			stateConditions = append(stateConditions, getConditionForColumn(column, include))
		}

		conditions = append(conditions, joinConditions(stateConditions, "AND", false))
	}

	havingConditions := joinConditions(conditions, "OR", true)

	if havingConditions != "" {
		sb.WriteString("HAVING ")
		sb.WriteString(havingConditions)
		sb.WriteString("\n")
	}
}

func addOrderByClause(sb *strings.Builder, newestFirst bool) {
	var order string
	if newestFirst {
		order = "DESC"
	}
	orderBy := fmt.Sprintf("ORDER BY job_id %s\n", order) // Job ids are sortable ULIDs
	sb.WriteString(orderBy)
}

func getConditionForColumn(column string, include bool) string {
	var condition string
	switch column {
	case submitted:
		condition = formatWithNot("MAX(job.submitted) IS %s NULL", include)
	case cancelled:
		condition = formatWithNot("MAX(job.cancelled) IS %s NULL", include)
	case created:
		condition = formatWithNot("MAX(job_run.created) IS %s NULL", include)
	case started:
		condition = formatWithNot("MAX(job_run.started) IS %s NULL", include)
	case finished:
		condition = formatWithNot("MAX(job_run.finished) IS %s NULL", include)
	case succeeded:
		condition = formatWithNot("%s BOOL_OR(job_run.succeeded)", !include)
	}
	return condition
}

func formatWithNot(condition string, addNot bool) string {
	not := ""
	if addNot {
		not = "NOT"
	}
	return fmt.Sprintf(condition, not)
}

func joinConditions(conditions []string, operator string, addParens bool) string {
	if len(conditions) == 0 {
		return ""
	}

	separator := fmt.Sprintf(" %s ", operator)
	if addParens {
		separator = fmt.Sprintf(") %s (", operator)
	}

	joined := strings.Join(conditions, separator)
	if addParens {
		joined = fmt.Sprintf("(%s)", joined)
	}
	return joined
}

func parseJobsInQueueRows(rows []*joinedRow) []*lookout.JobInfo {
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
