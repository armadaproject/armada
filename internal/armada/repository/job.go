package repository

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/common/util"
)

const jobObjectPrefix = "Job:"
const jobQueuePrefix = "Job:Queue:"
const jobSetPrefix = "Job:Set:"
const jobLeasedPrefix = "Job:Leased:"
const jobClusterMapKey = "Job:ClusterId"

type JobRepository interface {
	CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) []*api.Job
	AddJobs(job []*api.Job) ([]*SubmitJobResult, error)
	GetExistingJobsByIds(ids []string) ([]*api.Job, error)
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error)
	GetQueueSizes(queues []*api.Queue) (sizes []int64, e error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
	RenewLease(clusterId string, jobIds []string) (renewed []string, e error)
	ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error)
	ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error)
	DeleteJobs(jobs []*api.Job) map[*api.Job]error
	GetActiveJobIds(queue string, jobSetId string) ([]string, error)
}

type RedisJobRepository struct {
	db redis.UniversalClient
}

func NewRedisJobRepository(db redis.UniversalClient) *RedisJobRepository {
	return &RedisJobRepository{db: db}
}

func (repo *RedisJobRepository) CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) []*api.Job {
	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	for _, item := range request.JobRequestItems {
		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}

		j := &api.Job{
			Id:       util.NewULID(),
			Queue:    request.Queue,
			JobSetId: request.JobSetId,

			Namespace:   namespace,
			Labels:      item.Labels,
			Annotations: item.Annotations,

			RequiredNodeLabels: item.RequiredNodeLabels,

			Priority: item.Priority,

			PodSpec: item.PodSpec,
			Created: time.Now(),
			Owner:   principal.GetName(),
		}
		jobs = append(jobs, j)
	}

	return jobs
}

type submitJobRedisResponse struct {
	job               *api.Job
	queueJobResult    *redis.IntCmd
	saveJobResult     *redis.StatusCmd
	jobSetIndexResult *redis.IntCmd
}

type SubmitJobResult struct {
	Job   *api.Job
	Error error
}

func (repo *RedisJobRepository) AddJobs(jobs []*api.Job) ([]*SubmitJobResult, error) {
	pipe := repo.db.Pipeline()

	submitResults := make([]*submitJobRedisResponse, 0, len(jobs))

	for _, job := range jobs {
		submitResult := &submitJobRedisResponse{job: job}

		jobData, e := proto.Marshal(job)
		if e != nil {
			return nil, e
		}

		submitResult.queueJobResult =
			pipe.ZAdd(jobQueuePrefix+job.Queue, redis.Z{
				Member: job.Id,
				Score:  job.Priority},
			)

		submitResult.saveJobResult = pipe.Set(jobObjectPrefix+job.Id, jobData, 0)
		submitResult.jobSetIndexResult = pipe.SAdd(jobSetPrefix+job.JobSetId, job.Id)
		submitResults = append(submitResults, submitResult)
	}

	_, _ = pipe.Exec() // ignoring error here as it will be part of individual commands

	result := make([]*SubmitJobResult, 0, len(jobs))
	for _, submitResult := range submitResults {
		response := &SubmitJobResult{Job: submitResult.job}

		if _, e := submitResult.queueJobResult.Result(); e != nil {
			response.Error = e
		}
		if _, e := submitResult.saveJobResult.Result(); e != nil {
			response.Error = e
		}

		if _, e := submitResult.jobSetIndexResult.Result(); e != nil {
			response.Error = e
		}

		result = append(result, response)
	}

	return result, nil
}

func (repo *RedisJobRepository) RenewLease(clusterId string, jobIds []string) (renewedJobIds []string, e error) {
	jobs, e := repo.GetExistingJobsByIds(jobIds)
	if e != nil {
		return nil, e
	}
	return repo.leaseJobs(clusterId, jobs)
}

func (repo *RedisJobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	jobs, e := repo.GetExistingJobsByIds([]string{jobId})
	if e != nil {
		return nil, e
	}
	if len(jobs) == 0 {
		return nil, fmt.Errorf("Job not found %s", jobId)
	}
	job := jobs[0]

	returned, e := returnLease(repo.db, clusterId, job.Queue, job.Id, job.Created).Int()
	if e != nil {
		return nil, e
	}
	if returned > 0 {
		return job, nil
	}
	return nil, nil
}

type deleteJobRedisResponse struct {
	job                     *api.Job
	expiryAlreadySet        bool
	removeFromLeasedResult  *redis.IntCmd
	removeFromQueueResult   *redis.IntCmd
	setJobExpiryResult      *redis.BoolCmd
	deleteJobSetIndexResult *redis.IntCmd
}

func (repo *RedisJobRepository) DeleteJobs(jobs []*api.Job) map[*api.Job]error {
	expiryStatus := repo.getExpiryStatus(jobs)
	pipe := repo.db.Pipeline()
	deletionResults := make([]*deleteJobRedisResponse, 0, len(jobs))
	for _, job := range jobs {
		deletionResult := &deleteJobRedisResponse{job: job, expiryAlreadySet: expiryStatus[job]}
		deletionResult.removeFromQueueResult = pipe.ZRem(jobQueuePrefix+job.Queue, job.Id)
		deletionResult.removeFromLeasedResult = pipe.ZRem(jobLeasedPrefix+job.Queue, job.Id)
		deletionResult.deleteJobSetIndexResult = pipe.SRem(jobSetPrefix+job.JobSetId, job.Id)

		if !deletionResult.expiryAlreadySet {
			deletionResult.setJobExpiryResult = pipe.Expire(jobObjectPrefix+job.Id, time.Hour*24*7)
		}
		deletionResults = append(deletionResults, deletionResult)
	}
	_, _ = pipe.Exec() // ignoring error here as it will be part of individual commands

	cancelledJobs := map[*api.Job]error{}
	for _, deletionResult := range deletionResults {
		numberOfUpdates, err := processDeletionResponse(deletionResult)

		if numberOfUpdates > 0 {
			cancelledJobs[deletionResult.job] = nil
		}

		if err != nil {
			cancelledJobs[deletionResult.job] = err
		}
	}

	return cancelledJobs
}

// Returns details on if the expiry for each job is already set or not
func (repo *RedisJobRepository) getExpiryStatus(jobs []*api.Job) map[*api.Job]bool {
	pipe := repo.db.Pipeline()

	var cmds []*redis.DurationCmd
	for _, job := range jobs {
		cmds = append(cmds, pipe.TTL(jobObjectPrefix+job.Id))
	}
	_, _ = pipe.Exec() // ignoring error here as it will be part of individual commands

	expiryStatus := make(map[*api.Job]bool, len(jobs))
	for index, response := range cmds {
		expiry, err := response.Result()
		job := jobs[index]

		expiryStatus[job] = false
		if err == nil && expiry > 0 {
			expiryStatus[job] = true
		}
	}

	return expiryStatus
}

func processDeletionResponse(deletionResponse *deleteJobRedisResponse) (int64, error) {
	var totalUpdates int64 = 0
	var errorMessage error = nil

	modified, e := deletionResponse.removeFromLeasedResult.Result()
	totalUpdates += modified
	if e != nil {
		errorMessage = e
	}

	modified, e = deletionResponse.removeFromQueueResult.Result()
	totalUpdates += modified
	if e != nil {
		errorMessage = e
	}

	modified, e = deletionResponse.deleteJobSetIndexResult.Result()
	totalUpdates += modified
	if e != nil {
		errorMessage = e
	}

	if !deletionResponse.expiryAlreadySet {
		expirySet, e := deletionResponse.setJobExpiryResult.Result()
		if expirySet {
			totalUpdates++
		}
		if e != nil {
			errorMessage = e
		}
	}

	return totalUpdates, errorMessage
}

func (repo *RedisJobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	ids, e := repo.db.ZRange(jobQueuePrefix+queue, 0, limit-1).Result()
	if e != nil {
		return nil, e
	}
	return repo.GetExistingJobsByIds(ids)
}

// returns list of jobs which are successfully leased
func (repo *RedisJobRepository) TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error) {
	jobById := map[string]*api.Job{}
	for _, job := range jobs {
		jobById[job.Id] = job
	}

	leasedIds, e := repo.leaseJobs(clusterId, jobs)
	if e != nil {
		return nil, e
	}

	leasedJobs := make([]*api.Job, 0)
	for _, id := range leasedIds {
		leasedJobs = append(leasedJobs, jobById[id])
	}
	return leasedJobs, nil
}

// Returns existing jobs by Id
// If an Id is supplied that no longer exists, that job will simply be omitted from the result and no error returned
func (repo *RedisJobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	pipe := repo.db.Pipeline()
	var cmds []*redis.StringCmd
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(jobObjectPrefix+id))
	}
	_, e := pipe.Exec()

	var jobs []*api.Job
	for index, cmd := range cmds {
		_, err := cmd.Result()
		if err != nil {
			if err != redis.Nil {
				log.Warnf("No job found with with job id %s", ids[index])
			} else {
				return nil, err
			}
		}
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

func (repo *RedisJobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
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

func (repo *RedisJobRepository) GetQueueSizes(queues []*api.Queue) (sizes []int64, err error) {
	pipe := repo.db.Pipeline()
	cmds := []*redis.IntCmd{}
	for _, queue := range queues {
		cmds = append(cmds, pipe.ZCount(jobQueuePrefix+queue.Name, "-Inf", "+Inf"))
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	sizes = []int64{}
	for _, cmd := range cmds {
		sizes = append(sizes, cmd.Val())
	}
	return sizes, nil
}

func (repo *RedisJobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {

	queuedIds, e := repo.db.ZRange(jobQueuePrefix+queue, 0, -1).Result()
	if e != nil {
		return nil, e
	}
	leasedIds, e := repo.db.ZRange(jobLeasedPrefix+queue, 0, -1).Result()
	if e != nil {
		return nil, e
	}
	jobSetIds, e := repo.db.SMembers(jobSetPrefix + jobSetId).Result()
	if e != nil {
		return nil, e
	}

	activeIds := util.StringListToSet(append(queuedIds, leasedIds...))
	activeSetIds := []string{}
	for _, id := range jobSetIds {
		if activeIds[id] {
			activeSetIds = append(activeSetIds, id)
		}
	}
	return activeSetIds, nil
}

func (repo *RedisJobRepository) ExpireLeases(queue string, deadline time.Time) ([]*api.Job, error) {
	maxScore := strconv.FormatInt(deadline.UnixNano(), 10)

	// TODO: expire just limited number here ???
	ids, e := repo.db.ZRangeByScore(jobLeasedPrefix+queue, redis.ZRangeBy{Max: maxScore, Min: "-Inf"}).Result()
	if e != nil {
		return nil, e
	}
	expiringJobs, e := repo.GetExistingJobsByIds(ids)
	if e != nil {
		return nil, e
	}

	expired := make([]*api.Job, 0)
	if len(expiringJobs) == 0 {
		return expired, nil
	}

	cmds := make(map[*api.Job]*redis.Cmd)

	pipe := repo.db.Pipeline()
	expireScript.Load(pipe)
	for _, job := range expiringJobs {
		cmds[job] = expire(pipe, job.Queue, job.Id, job.Created, deadline)
	}
	_, e = pipe.Exec()

	if e != nil {
		return nil, e
	}

	for job, cmd := range cmds {
		value, e := cmd.Int()
		if e != nil {
			log.Error(e)
		} else if value > 0 {
			expired = append(expired, job)
		}
	}
	return expired, nil
}

func (repo *RedisJobRepository) leaseJobs(clusterId string, jobs []*api.Job) ([]string, error) {

	now := time.Now()
	pipe := repo.db.Pipeline()

	leaseJobScript.Load(pipe)

	cmds := make(map[string]*redis.Cmd)
	for _, job := range jobs {
		cmds[job.Id] = leaseJob(pipe, job.Queue, clusterId, job.Id, now)
	}
	_, e := pipe.Exec()
	if e != nil {
		return nil, e
	}

	leasedJobs := make([]string, 0)
	for jobId, cmd := range cmds {
		value, e := cmd.Int()
		if e != nil {
			log.Error(e)
		} else if value == alreadyAllocatedByDifferentCluster {
			log.WithField("jobId", jobId).Info("Job Already allocated to different cluster")
		} else if value == jobCancelled {
			log.WithField("jobId", jobId).Info("Trying to renew cancelled job")
		} else {
			leasedJobs = append(leasedJobs, jobId)
		}
	}
	return leasedJobs, nil
}

func leaseJob(db redis.Cmdable, queueName string, clusterId string, jobId string, now time.Time) *redis.Cmd {
	return leaseJobScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		clusterId, jobId, float64(now.UnixNano()))
}

const alreadyAllocatedByDifferentCluster = -42
const jobCancelled = -43

var leaseJobScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]
local clusterAssociation = KEYS[3]

local clusterId = ARGV[1]
local jobId = ARGV[2]
local currentTime = ARGV[3]

local exists = redis.call('ZREM', queue, jobId)

if exists == 1 then 
	redis.call('HSET', clusterAssociation, jobId, clusterId)
	return redis.call('ZADD', leasedJobsSet, currentTime, jobId)
else
	local currentClusterId = redis.call('HGET', clusterAssociation, jobId)
	local score = redis.call('ZSCORE', leasedJobsSet, jobId)
	
	if currentClusterId ~= clusterId then
		return -42
	end

	if score == false then
		return -43
	end

	return redis.call('ZADD', leasedJobsSet, currentTime, jobId)
end
`)

func expire(db redis.Cmdable, queueName string, jobId string, created time.Time, deadline time.Time) *redis.Cmd {
	return expireScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName},
		jobId, float64(created.UnixNano()), float64(deadline.UnixNano()))
}

var expireScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]

local jobId = ARGV[1]
local created = tonumber(ARGV[2])
local deadline = tonumber(ARGV[3])

local leasedTime = tonumber(redis.call('ZSCORE', leasedJobsSet, jobId))

if leasedTime ~= nil and leasedTime < deadline then
	local exists = redis.call('ZREM', leasedJobsSet, jobId)
	if exists ~= 0 then
		return redis.call('ZADD', queue, created, jobId)
	else
		return 0
	end
end
`)

func returnLease(db redis.Cmdable, clusterId string, queueName string, jobId string, created time.Time) *redis.Cmd {
	return returnLeaseScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		clusterId, jobId, float64(created.UnixNano()))
}

var returnLeaseScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]
local clusterAssociation = KEYS[3]

local clusterId = ARGV[1]
local jobId = ARGV[2]
local created = tonumber(ARGV[3])

local currentClusterId = redis.call('HGET', clusterAssociation, jobId)

if currentClusterId == clusterId then
	local exists = redis.call('ZREM', leasedJobsSet, jobId)
	if exists ~= 0 then
		return redis.call('ZADD', queue, created, jobId)
	else
		return 0
	end
end
return 0
`)
