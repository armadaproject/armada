package repository

import (
	"database/sql"

	"github.com/G-Research/armada/pkg/api/lookout"
)

type JobRepository interface {
	GetQueueStats() ([]*lookout.QueueInfo, error)
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
