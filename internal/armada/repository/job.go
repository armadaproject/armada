package repository

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"time"
)

const jobObjectPrefix = "Job:"
const jobQueuePrefix = "Job:Queue:"
const jobLeasedPrefix = "Job:Leased:"
const jobClusterMapKey = "Job:ClusterId"

type JobRepository interface {
	CreateJob(request *api.JobRequest) *api.Job
	AddJob(job *api.Job) error
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
	RenewLease(clusterId string, jobIds []string) (unsuccessful []string, e error)
	Remove(jobIds []string) (unsuccessful []string, e error)
}

type RedisJobRepository struct {
	db *redis.Client
}

func (repo RedisJobRepository) CreateJob(request *api.JobRequest) *api.Job {
	j := api.Job{
		Id:       util.NewULID(),
		Queue:    request.Queue,
		JobSetId: request.JobSetId,

		Priority: request.Priority,

		PodSpec: request.PodSpec,
		Created: time.Now(),
	}
	return &j
}

func (repo RedisJobRepository) RenewLease(clusterId string, jobIds []string) (unsuccessful []string, e error) {
	// TODO
	return nil, nil
}

func (repo RedisJobRepository) Remove(jobIds []string) (unsuccessful []string, e error) {
	// TODO
	return nil, nil
}

func NewRedisJobRepository(db *redis.Client) *RedisJobRepository {
	return &RedisJobRepository{db: db}
}

func (repo RedisJobRepository) AddJob(job *api.Job) error {

	pipe := repo.db.TxPipeline()

	jobData, e := proto.Marshal(job)
	if e != nil {
		return e
	}

	pipe.ZAdd(jobQueuePrefix+job.Queue, redis.Z{
		Member: job.Id,
		Score:  job.Priority})

	pipe.Set(jobObjectPrefix+job.Id, jobData, 0)

	_, e = pipe.Exec()
	return e
}

func (repo RedisJobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	ids, e := repo.db.ZRange(jobQueuePrefix+queue, 0, limit-1).Result()
	if e != nil {
		return nil, e
	}
	return repo.GetJobsByIds(ids)
}

// returns list of jobs which are successfully leased
func (repo RedisJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	if len(jobs) <= 0 {
		return []*api.Job{}, nil
	}

	now := float64(time.Now().Unix())
	pipe := repo.db.Pipeline()
	cmds := make(map[*api.Job]*redis.Cmd)
	for _, job := range jobs {
		cmds[job] = zmove(pipe, jobQueuePrefix+queue, jobLeasedPrefix+queue, job.Id, now)
	}

	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	leasedJobs := make([]*api.Job, 0)
	for job, cmd := range cmds {
		modified, e := cmd.Int()
		if e == nil && modified > 0 {
			leasedJobs = append(leasedJobs, job)
		}
	}

	leasedJobsClusterIds := make(map[string]interface{})
	for _, job := range leasedJobs {
		leasedJobsClusterIds[job.Id] = clusterId
	}
	e = repo.db.HMSet(jobClusterMapKey, leasedJobsClusterIds).Err()
	if e != nil {
		// TODO: try cancelling the lease, or implement this all as script?
		return nil, e
	}

	return leasedJobs, nil
}

func (repo RedisJobRepository) GetJobsByIds(ids []string) ([]*api.Job, error) {
	pipe := repo.db.Pipeline()
	var cmds []*redis.StringCmd
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(jobObjectPrefix+id))
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	var jobs []*api.Job
	for _, cmd := range cmds {
		d, _ := cmd.Bytes()
		job := &api.Job{}
		e = proto.Unmarshal(d, job)
		if e != nil {
			return nil, e
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (repo RedisJobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
	pipe := repo.db.Pipeline()
	cmds := make(map[*api.Queue]*redis.IntCmd)
	for _, queue := range queues {
		// empty (even sorted) sets gets deleted by redis automatically
		cmds[queue] = pipe.Exists(jobQueuePrefix + queue.Name)
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	var active []*api.Queue
	for queue, cmd := range cmds {
		if cmd.Val() > 0 {
			active = append(active, queue)
		}
	}
	return active, nil
}

const zmoveScript = `
local exists = redis.call('ZREM', KEYS[1], ARGV[1])
if exists then
	return redis.call('ZADD', KEYS[2], ARGV[2], ARGV[1])
else
	return 0
end
`

func zmove(db redis.Cmdable, from string, to string, key string, score float64) *redis.Cmd {
	return db.Eval(zmoveScript, []string{from, to}, key, score)
}
