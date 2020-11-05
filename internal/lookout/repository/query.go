package repository

import (
	"database/sql"
	"fmt"
	"github.com/G-Research/armada/pkg/api"
	"time"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type JobRepository interface {
	GetQueueStats() ([]*lookout.QueueInfo, error)
	GetQueuedJobs(queue string) ([]*lookout.JobInfo, error)
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
	RunId     string
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

func (r *SQLJobRepository) GetQueuedJobs(queue string) ([]*lookout.JobInfo, error) {
	queryString := `
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
		WHERE job.job_id IN (
			SELECT job.job_id
			FROM job
			WHERE job.queue = '%s'
		)`
	rows, err := r.db.Query(fmt.Sprintf(queryString, queue))
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

	jobRunMap := make(map[string]*lookout.JobInfo)

	for _, row := range joinedRows {
		if _, ok := jobRunMap[row.JobId]; !ok {
			jobRunMap[row.JobId] = &lookout.JobInfo{
				Job:  &api.Job{
					Id:                 row.JobId,
					JobSetId:           row.JobSet,
					Queue:              row.Queue,
					Namespace:          "",
					Labels:             nil,
					Annotations:        nil,
					Owner:              row.Owner,
					Priority:           ParseNullFloat(row.Priority),
					PodSpec:            nil,
					Created:            ParseNullTimeDefault(row.Submitted),
				},
				Runs: []*lookout.RunInfo{},
			}
		}

		jobRunMap[row.JobId].Runs = append(jobRunMap[row.JobId].Runs, &lookout.RunInfo{
			K8SId:     row.RunId,
			Cluster:   ParseNullString(row.Cluster),
			Node:      ParseNullString(row.Node),
			Succeeded: ParseNullBool(row.Succeeded),
			Error:     ParseNullString(row.Error),
			Submitted: ParseNullTime(row.Submitted),
			Cancelled: ParseNullTime(row.Cancelled),
			Started:   ParseNullTime(row.Started),
			Finished:  ParseNullTime(row.Finished),
		})
	}

	result := make([]*lookout.JobInfo, 0)
	for _, jobInfo := range jobRunMap {
		result = append(result, jobInfo)
	}
	return result, nil
}

func ParseNullString(nullString sql.NullString) string {
	if !nullString.Valid {
		return ""
	}
	return nullString.String
}

func ParseNullBool(nullBool sql.NullBool) bool {
	if !nullBool.Valid {
		return false
	}
	return nullBool.Bool
}

func ParseNullFloat(nullFloat sql.NullFloat64) float64 {
	if !nullFloat.Valid {
		return 0
	}
	return nullFloat.Float64
}

func ParseNullTime(nullTime sql.NullTime) *time.Time {
	if !nullTime.Valid {
		return nil
	}
	return &nullTime.Time
}

func ParseNullTimeDefault(nullTime sql.NullTime) time.Time {
	if !nullTime.Valid {
		return time.Time{}
	}
	return nullTime.Time
}
