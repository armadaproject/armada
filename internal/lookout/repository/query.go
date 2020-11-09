package repository

import (
	"database/sql"
	"fmt"
	"github.com/G-Research/armada/pkg/api"
	"strings"
	"time"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type JobRepository interface {
	GetQueueStats() ([]*lookout.QueueInfo, error)
	GetJobsInQueue(queue string, opts GetJobsInQueueOpts) ([]*lookout.JobInfo, error)
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
	Submitted sql.NullTime
	Cancelled sql.NullTime
	JobJson   sql.NullString
	RunId     sql.NullString
	Cluster   sql.NullString
	Node      sql.NullString
	Created   sql.NullTime
	Started   sql.NullTime
	Finished  sql.NullTime
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

type GetJobsInQueueOpts struct {
	NewestFirst bool
	FilterStates []JobState
}

type JobState string

const (
	Queued    = "Queued" // Not leased yet
	Pending   = "Pending"
	Running   = "Running"
	Succeeded = "Succeeded"
	Failed    = "Failed"
	Cancelled = "Cancelled"
)

func (r *SQLJobRepository) GetJobsInQueue(queue string, opts GetJobsInQueueOpts) ([]*lookout.JobInfo, error) {
	queryString := makeGetJobsInQueueQuery(queue, opts)
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

	result := make([]*lookout.JobInfo, 0)

	for i, row := range joinedRows {
		if i == 0 || result[len(result) - 1].Job.Id != row.JobId {
			result = append(result, &lookout.JobInfo{
				Job:  &api.Job{
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

	return result, nil
}

const (
	submitted = "job.submitted"
	cancelled = "job.cancelled"
	created = "job_run.created"
	started = "job_run.started"
	finished = "job_run.finished"
	succeeded = "job_run.succeeded"
)

var stateFiltersMap = map[JobState]map[string]bool {
	Queued: {
		submitted: true,
		cancelled: false,
		created: false,
		started: false,
		finished: false,
	},
	Pending: {
		cancelled: false,
		created: true,
		started: false,
		finished: false,
	},
	Running: {
		cancelled: false,
		started: true,
		finished: false,
	},
	Succeeded: {
		cancelled: false,
		finished: true,
		succeeded: true,
	},
	Failed: {
		succeeded: false,
	},
	Cancelled: {
		cancelled: true,
	},
}

func formatWithNot(condition string, addNot bool) string {
	not := ""
	if addNot {
		not = "NOT"
	}
	return fmt.Sprintf(condition, not)
}

func makeGetJobsInQueueQuery(queue string, opts GetJobsInQueueOpts) string {
	hs := make([]string, 0)

	for _, state := range opts.FilterStates {
		h := make([]string, 0)

		for column, include := range stateFiltersMap[state] {
			if column == submitted {
				h = append(h, formatWithNot("MAX(job.submitted) IS %s NULL", include))
			} else if column == cancelled {
				h = append(h, formatWithNot("MAX(job.cancelled) IS %s NULL", include))
			} else if column == created {
				h = append(h, formatWithNot("MAX(job_run.created) IS %s NULL", include))
			} else if column == started {
				h = append(h, formatWithNot("MAX(job_run.started) IS %s NULL", include))
			} else if column == finished {
				h = append(h, formatWithNot("MAX(job_run.finished) IS %s NULL", include))
			} else if column == succeeded {
				h = append(h, formatWithNot("%s BOOL_OR(job_run.succeeded)", !include))
			}
		}

		if len(h) > 0 {
			hs = append(hs, strings.Join(h, " AND "))
		}
	}

	var havingClause string

	if len(hs) > 0 {
		havingClause = fmt.Sprintf("(%s)", strings.Join(hs, ") OR ("))
	}

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

	sb := &strings.Builder{}
	sb.WriteString(`
		SELECT job.job_id
		FROM job LEFT JOIN job_run ON job.job_id = job_run.job_id
	`)

	sb.WriteString(fmt.Sprintf("WHERE job.queue  = '%s'\n", queue))

	sb.WriteString("GROUP BY job.job_id\n")

	if havingClause != "" {
		sb.WriteString("HAVING ")
		sb.WriteString(havingClause)
		sb.WriteString("\n")
	}

	var order string
	if opts.NewestFirst {
		order = "DESC"
	} else {
		order = "ASC"
	}
	orderBy := fmt.Sprintf("ORDER BY job_id %s\n", order) // Job ids are sortable ULIDs
	sb.WriteString(orderBy)

	return fmt.Sprintf(query, sb.String())
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

func parseNullTime(nullTime sql.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func parseNullTimeDefault(nullTime sql.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Time{}
	}
	return nullTime.Time
}
