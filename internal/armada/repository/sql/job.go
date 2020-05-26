package sql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/G-Research/armada/internal/armada/repository/redis"
	"github.com/G-Research/armada/pkg/api"
)

type JobRepository struct {
	db sql.DB
}

func (j *JobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	rows, err := j.db.Query(
		`SELECT job FROM job_queue WHERE queue = $1 AND cluster IS NULL ORDER BY priority, created LIMIT $2 `,
		queue, limit)

	if err != nil {
		return nil, err
	}
	return readJobs(rows)
}

func (j *JobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	ids := []string{}
	jobById := map[string]*api.Job{}
	for _, j := range jobs {
		ids = append(ids, j.Id)
		jobById[j.Id] = j
	}

	rows, err := j.db.Query(`
		UPDATE job_queue
		SET cluster = $2
		WHERE id IN $1 AND cluster IS NULL OR cluster = $2
		RETURNING id`, ids, clusterId)

	if err != nil {
		return nil, err
	}

	leasedJobs := []*api.Job{}
	for rows.Next() {
		var id string
		err := rows.Scan(&id)
		if err != nil {
			return nil, err
		}

		leasedJobs = append(leasedJobs, jobById[id])
	}
	return leasedJobs, nil
}

func (j *JobRepository) AddJobs(job []*api.Job) ([]*redis.SubmitJobResult, error) {
	panic("implement me")
}

func (j *JobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	panic("implement me")
}

func (j *JobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
	panic("implement me")
}

func (j *JobRepository) GetQueueSizes(queues []*api.Queue) (sizes []int64, e error) {
	panic("implement me")
}

func (j *JobRepository) RenewLease(clusterId string, jobIds []string) (renewed []string, e error) {
	panic("implement me")
}

func (j *JobRepository) ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error) {
	panic("implement me")
}

func (j *JobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	panic("implement me")
}

func (j *JobRepository) DeleteJobs(jobs []*api.Job) map[*api.Job]error {
	panic("implement me")
}

func (j *JobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	panic("implement me")
}

func (j *JobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {
	panic("implement me")
}

func readJobs(rows *sql.Rows) ([]*api.Job, error) {
	jobs := []*api.Job{}
	for rows.Next() {
		var value interface{}
		err := rows.Scan(&value)
		if err != nil {
			return nil, err
		}
		b, ok := value.([]byte)
		if !ok {
			return nil, fmt.Errorf("could not read job from database")
		}
		var j *api.Job
		err = json.Unmarshal(b, &j)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal job from database")
		}
		jobs = append(jobs, j)
	}
	return jobs, nil
}
