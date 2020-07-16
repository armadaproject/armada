package sql

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/G-Research/armada/internal/armada/repository/redis"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"

	"github.com/lib/pq"
)

type JobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) *JobRepository {
	return &JobRepository{db: db}
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
		SET cluster = $2, leased = $3
		WHERE id IN (
		    SELECT id FROM job_queue 
		    WHERE id = ANY($1) AND (cluster IS NULL OR cluster = $2)
		    FOR NO KEY UPDATE SKIP LOCKED)
		RETURNING id`, pq.Array(ids), clusterId, time.Now())

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

func (j *JobRepository) AddJobs(jobs []*api.Job) ([]*redis.SubmitJobResult, error) {

	values := []interface{}{}
	for _, j := range jobs {
		jobData, err := json.Marshal(j)
		if err != nil {
			return nil, err
		}
		values = append(values, j.Id, j.Queue, j.JobSetId, j.Created, j.Priority, jobData)
	}

	_, err := insert(j.db, "job_queue", []string{"id", "queue", "jobset", "created", "priority", "job"}, values)
	if err != nil {
		return nil, err
	}

	result := []*redis.SubmitJobResult{}
	for _, j := range jobs {
		result = append(result, &redis.SubmitJobResult{Job: j})
	}

	return result, nil
}

func (j *JobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	rows, err := j.db.Query(`SELECT job FROM job_queue WHERE id = Any($1)`, pq.Array(ids))
	if err != nil {
		return nil, err
	}
	return readJobs(rows)
}

func (j *JobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
	names := []string{}
	for _, q := range queues {
		names = append(names, q.Name)
	}
	rows, err := j.db.Query("SELECT DISTINCT queue from job_queue WHERE queue = Any($1)", pq.Array(names))
	if err != nil {
		return nil, err
	}

	queueNames, err := readStrings(rows)
	if err != nil {
		return nil, err
	}

	namesLookup := util.StringListToSet(queueNames)
	result := []*api.Queue{}

	for _, q := range queues {
		if namesLookup[q.Name] {
			result = append(result, q)
		}

	}
	return result, nil
}

func (j *JobRepository) GetQueueSizes(queues []*api.Queue) (sizes []int64, e error) {
	names := []string{}
	for _, q := range queues {
		names = append(names, q.Name)
	}
	rows, err := j.db.Query("SELECT queue, count(*) from job_queue WHERE queue = Any($1) AND cluster IS NULL GROUP BY queue", pq.Array(names))
	if err != nil {
		return nil, err
	}

	queueSizes := map[string]int64{}
	for rows.Next() {
		var queueName string
		var size int64
		err := rows.Scan(&queueName, &size)
		if err != nil {
			return nil, err
		}
		queueSizes[queueName] = size
	}
	result := []int64{}
	for _, q := range queues {
		result = append(result, queueSizes[q.Name])
	}
	return result, nil
}

func (j *JobRepository) RenewLease(clusterId string, jobIds []string) (renewed []string, e error) {
	now := time.Now()

	rows, err := j.db.Query(`
		UPDATE job_queue
		SET leased = $1, cluster = $3
		WHERE id = ANY($2) AND (cluster IS NULL OR cluster = $3)
		RETURNING id`, now, pq.Array(jobIds), clusterId)

	if err != nil {
		return nil, err
	}
	return readStrings(rows)
}

func (j *JobRepository) ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error) {
	rows, err := j.db.Query(`
		UPDATE job_queue
		SET cluster = NULL
		WHERE queue = $1 AND leased <= $2
		RETURNING job`, queue, deadline)

	if err != nil {
		return nil, err
	}
	return readJobs(rows)
}

func (j *JobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	rows, err := j.db.Query(`
		UPDATE job_queue
		SET cluster = NULL, leased = NULL
		WHERE id = $1 AND cluster = $2
		RETURNING job`, jobId, clusterId)

	if err != nil {
		return nil, err
	}
	jobs, err := readJobs(rows)

	if len(jobs) == 0 {
		// to keep compatibility with redis implementation, return nil when no job was updated
		return nil, nil
	}
	return jobs[0], nil
}

func (j *JobRepository) DeleteJobs(jobs []*api.Job) map[*api.Job]error {
	ids := []string{}
	jobsById := map[string]*api.Job{}
	for _, j := range jobs {
		ids = append(ids, j.Id)
		jobsById[j.Id] = j
	}

	rows, err := j.db.Query(`
		DELETE FROM job_queue
		WHERE id = ANY($1)
		RETURNING id`, pq.Array(ids))

	if err != nil {
		return allJobsFailed(jobs, err)
	}

	updatedIds, err := readStrings(rows)
	if err != nil {
		return allJobsFailed(jobs, err)
	}

	result := map[*api.Job]error{}
	for _, id := range updatedIds {
		result[jobsById[id]] = nil
	}
	return result
}

func allJobsFailed(jobs []*api.Job, err error) map[*api.Job]error {
	result := map[*api.Job]error{}
	for _, j := range jobs {
		result[j] = err
	}
	return result
}

func (j *JobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	rows, err := j.db.Query(
		`SELECT id FROM job_queue WHERE queue = $1 AND jobset = $2`,
		queue, jobSetId)

	if err != nil {
		return nil, err
	}
	return readStrings(rows)
}

func (j *JobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {
	rows, err := j.db.Query(
		`SELECT jobset, (cluster IS NULL), count(*) FROM job_queue WHERE queue = $1 GROUP BY jobset, (cluster IS NULL)`,
		queue)

	if err != nil {
		return nil, err
	}

	jobSets := map[string]*api.JobSetInfo{}

	for rows.Next() {
		var name string
		var leased bool
		var size int32

		err := rows.Scan(&name, &leased, &size)
		if err != nil {
			return nil, err
		}

		jobSet, ok := jobSets[name]
		if !ok {
			jobSet = &api.JobSetInfo{Name: name}
			jobSets[name] = jobSet
		}

		if leased {
			jobSet.LeasedJobs = size
		} else {
			jobSet.QueuedJobs = size
		}
	}

	result := []*api.JobSetInfo{}
	for _, s := range jobSets {
		result = append(result, s)
	}
	return result, nil
}

func readJobs(rows *sql.Rows) ([]*api.Job, error) {
	jobs := []*api.Job{}
	err := readByteRows(rows, func(b []byte) error {
		var j *api.Job
		err := json.Unmarshal(b, &j)
		jobs = append(jobs, j)
		return err
	})
	return jobs, err
}
