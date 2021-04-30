package repository

import (
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/common/validation"
	"github.com/G-Research/armada/pkg/api"
)

const jobObjectPrefix = "Job:"
const jobStartTimePrefix = "Job:StartTime"
const jobQueuePrefix = "Job:Queue:"
const jobSetPrefix = "Job:Set:"
const jobLeasedPrefix = "Job:Leased:"
const jobClusterMapKey = "Job:ClusterId"
const jobRetriesPrefix = "Job:Retries:"
const jobClientIdPrefix = "job:ClientId:"
const keySeparator = ":"

const queueResourcesBatchSize = 20000

const JobNotFound = "no job found with provided Id"

type JobRepository interface {
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
	CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) ([]*api.Job, error)
	AddJobs(job []*api.Job) ([]*SubmitJobResult, error)
	GetExistingJobsByIds(ids []string) ([]*api.Job, error)
	FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error)
	GetQueueSizes(queues []*api.Queue) (sizes []int64, e error)
	IterateQueueJobs(queueName string, action func(*api.Job)) error
	GetQueueJobIds(queueName string) ([]string, error)
	RenewLease(clusterId string, jobIds []string) (renewed []string, e error)
	ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error)
	ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error)
	DeleteJobs(jobs []*api.Job) map[*api.Job]error
	GetActiveJobIds(queue string, jobSetId string) ([]string, error)
	GetLeasedJobIds(queue string) ([]string, error)
	UpdateStartTime(jobId string, clusterId string, startTime time.Time) error
	GetJobRunInfos(jobIds []string) (map[string]*RunInfo, error)
	GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error)
	AddRetryAttempt(jobId string) error
	GetNumberOfRetryAttempts(jobId string) (int, error)
}

type RedisJobRepository struct {
	db               redis.UniversalClient
	defaultJobLimits common.ComputeResources
}

func NewRedisJobRepository(db redis.UniversalClient, defaultJobLimits common.ComputeResources) *RedisJobRepository {
	if defaultJobLimits == nil {
		defaultJobLimits = common.ComputeResources{}
	}
	return &RedisJobRepository{db: db, defaultJobLimits: defaultJobLimits}
}

func (repo *RedisJobRepository) CreateJobs(request *api.JobSubmitRequest, principal authorization.Principal) ([]*api.Job, error) {
	jobs := make([]*api.Job, 0, len(request.JobRequestItems))

	if request.JobSetId == "" {
		return nil, fmt.Errorf("job set is not specified")
	}

	if request.Queue == "" {
		return nil, fmt.Errorf("queue is not specified")
	}

	for i, item := range request.JobRequestItems {
		if item.PodSpec != nil && len(item.PodSpecs) > 0 {
			return nil, fmt.Errorf("job with index %v has both pod spec and pod spec list specified", i)
		}

		if len(item.GetAllPodSpecs()) == 0 {
			return nil, fmt.Errorf("job with index %v has no pod spec", i)
		}

		namespace := item.Namespace
		if namespace == "" {
			namespace = "default"
		}

		for j, podSpec := range item.GetAllPodSpecs() {
			repo.applyDefaults(podSpec)
			e := validation.ValidatePodSpec(podSpec)
			if e != nil {
				return nil, fmt.Errorf("error validating pod spec of job with index %v, pod: %v: %v", i, j, e)
			}

			// TODO: remove, RequiredNodeLabels is deprecated and will be removed in future versions
			for k, v := range item.RequiredNodeLabels {
				if podSpec.NodeSelector == nil {
					podSpec.NodeSelector = map[string]string{}
				}
				podSpec.NodeSelector[k] = v
			}
		}

		j := &api.Job{
			Id:       util.NewULID(),
			ClientId: item.ClientId,
			Queue:    request.Queue,
			JobSetId: request.JobSetId,

			Namespace:   namespace,
			Labels:      item.Labels,
			Annotations: item.Annotations,

			RequiredNodeLabels: item.RequiredNodeLabels,

			Priority: item.Priority,

			PodSpec:  item.PodSpec,
			PodSpecs: item.PodSpecs,
			Created:  time.Now(),
			Owner:    principal.GetName(),
		}
		jobs = append(jobs, j)
	}

	return jobs, nil
}

type SubmitJobResult struct {
	JobId             string
	SubmittedJob      *api.Job
	DuplicateDetected bool
	Error             error
}

func (repo *RedisJobRepository) AddJobs(jobs []*api.Job) ([]*SubmitJobResult, error) {
	pipe := repo.db.Pipeline()

	addJobScript.Load(pipe)

	saveResults := make([]*redis.Cmd, 0, len(jobs))

	for _, job := range jobs {
		jobData, e := proto.Marshal(job)
		if e != nil {
			return nil, e
		}

		result := addJob(pipe, job, &jobData)
		saveResults = append(saveResults, result)
	}

	_, _ = pipe.Exec() // ignoring error here as it will be part of individual commands

	result := make([]*SubmitJobResult, 0, len(jobs))
	for i, saveResult := range saveResults {
		resultJobId, err := saveResult.String()
		submitJobResult := &SubmitJobResult{
			JobId:             resultJobId,
			SubmittedJob:      jobs[i],
			Error:             err,
			DuplicateDetected: resultJobId != jobs[i].Id,
		}
		result = append(result, submitJobResult)
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
	job                            *api.Job
	expiryAlreadySet               bool
	removeFromLeasedResult         *redis.IntCmd
	removeFromQueueResult          *redis.IntCmd
	removeClusterAssociationResult *redis.IntCmd
	removeStartTimeResult          *redis.IntCmd
	setJobExpiryResult             *redis.BoolCmd
	deleteJobSetIndexResult        *redis.IntCmd
	deleteJobRetriesResult         *redis.IntCmd
}

func (repo *RedisJobRepository) DeleteJobs(jobs []*api.Job) map[*api.Job]error {
	expiryStatus := repo.getExpiryStatus(jobs)
	pipe := repo.db.Pipeline()
	deletionResults := make([]*deleteJobRedisResponse, 0, len(jobs))
	for _, job := range jobs {
		deletionResult := &deleteJobRedisResponse{job: job, expiryAlreadySet: expiryStatus[job]}
		deletionResult.removeFromQueueResult = pipe.ZRem(jobQueuePrefix+job.Queue, job.Id)
		deletionResult.removeFromLeasedResult = pipe.ZRem(jobLeasedPrefix+job.Queue, job.Id)
		deletionResult.removeClusterAssociationResult = pipe.HDel(jobClusterMapKey, job.Id)
		deletionResult.removeStartTimeResult = pipe.Del(jobStartTimePrefix + job.Id)
		deletionResult.deleteJobSetIndexResult = pipe.SRem(jobSetPrefix+job.JobSetId, job.Id)
		deletionResult.deleteJobRetriesResult = pipe.Del(jobRetriesPrefix + job.Id)

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

	modified, e = deletionResponse.removeClusterAssociationResult.Result()
	totalUpdates += modified
	if e != nil {
		errorMessage = e
	}

	modified, e = deletionResponse.removeStartTimeResult.Result()
	totalUpdates += modified
	if e != nil {
		errorMessage = e
	}

	modified, e = deletionResponse.deleteJobRetriesResult.Result()
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
// If an Id is supplied that no longer exists, that job will simply be omitted from the result.
// No error will be thrown for missing jobs
func (repo *RedisJobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	pipe := repo.db.Pipeline()
	var cmds []*redis.StringCmd
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(jobObjectPrefix+id))
	}
	_, _ = pipe.Exec() // ignoring error here as it will be part of individual commands

	var jobs []*api.Job
	for index, cmd := range cmds {
		_, e := cmd.Result()
		if e != nil {
			if e == redis.Nil {
				log.Warnf("No job found with with job id %s", ids[index])
				continue
			} else {
				return nil, e
			}
		}
		d, _ := cmd.Bytes()
		job := &api.Job{}
		e = proto.Unmarshal(d, job)
		if e != nil {
			return nil, e
		}

		for _, podSpec := range job.GetAllPodSpecs() {
			// TODO: remove, RequiredNodeLabels is deprecated and will be removed in future versions
			for k, v := range job.RequiredNodeLabels {
				if podSpec.NodeSelector == nil {
					podSpec.NodeSelector = map[string]string{}
				}
				podSpec.NodeSelector[k] = v
			}
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

func (repo *RedisJobRepository) IterateQueueJobs(queueName string, action func(*api.Job)) error {
	queuedIds, e := repo.GetQueueJobIds(queueName)
	if e != nil {
		return e
	}
	for len(queuedIds) > 0 {
		take := queueResourcesBatchSize
		if len(queuedIds) < queueResourcesBatchSize {
			take = len(queuedIds)
		}
		queuedJobs, e := repo.GetExistingJobsByIds(queuedIds[0:take])
		queuedIds = queuedIds[take:]

		if e != nil {
			return e
		}
		for _, job := range queuedJobs {
			action(job)
		}

	}
	return nil
}

func (repo *RedisJobRepository) GetLeasedJobIds(queue string) ([]string, error) {
	return repo.db.ZRange(jobLeasedPrefix+queue, 0, -1).Result()
}

func (repo *RedisJobRepository) getAssociatedCluster(jobIds []string) (map[string]string, error) {
	associatedCluster := make(map[string]string, len(jobIds))
	pipe := repo.db.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(jobIds))

	for _, jobId := range jobIds {
		cmds[jobId] = pipe.HGet(jobClusterMapKey, jobId)
	}

	_, e := pipe.Exec()
	if e != nil && e != redis.Nil {
		return associatedCluster, e
	}

	for jobId, cmd := range cmds {
		err := cmd.Err()
		if err != nil && err != redis.Nil {
			return map[string]string{}, err
		}
		clusterId := cmd.Val()
		if clusterId != "" {
			associatedCluster[jobId] = cmd.Val()
		}
	}

	return associatedCluster, nil
}

func (repo *RedisJobRepository) UpdateStartTime(jobId string, clusterId string, startTime time.Time) error {
	// This is a bit naive, we set the start time to be associated with the job id + cluster id provided
	// We only save the earliest start time for each job id + cluster id combination
	// We have to save against cluster id to handle when a lease expires and a job starts on another cluster
	// However this is not full proof and in very rare situations a job could start twice on the same cluster and this value will be wrong
	// TODO When we have a proper concept of lease, associate start time with that specific lease (ideally earliest value for that lease)

	jobs, e := repo.GetExistingJobsByIds([]string{jobId})
	if e != nil {
		return e
	}

	if len(jobs) <= 0 {
		return fmt.Errorf(JobNotFound)
	}

	output := updateStartTimeScript.Run(repo.db, []string{jobStartTimePrefix + jobId, jobClusterMapKey}, clusterId, startTime.UnixNano())

	return output.Err()
}

var updateStartTimeScript = redis.NewScript(`
local startTimeKey = KEYS[1]
local clusterAssociation = KEYS[2]

local clusterId = ARGV[1]
local startTime = ARGV[2]
local startTimeNumber = tonumber(ARGV[2])

local currentStartTime = tonumber(redis.call('HGET', startTimeKey, clusterId))

if currentStartTime ~= nil and currentStartTime < startTimeNumber then
	return 0
end

return redis.call('HSET', startTimeKey, clusterId, startTime)
`)

type RunInfo struct {
	StartTime        time.Time
	CurrentClusterId string
}

/*
 Returns the run info of each job id for the cluster they are currently associated with (leased by)
 Jobs with no value will be omitted from the results, which happens in the following cases:
 - The job is not associated with a cluster
 - The job has does not have a start time for the cluster it is associated with
*/
func (repo *RedisJobRepository) GetJobRunInfos(jobIds []string) (map[string]*RunInfo, error) {
	runInfos := make(map[string]*RunInfo, len(jobIds))

	associatedClusters, err := repo.getAssociatedCluster(jobIds)
	if err != nil {
		return runInfos, err
	}

	pipe := repo.db.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(jobIds))

	for _, jobId := range jobIds {
		if clusterId, present := associatedClusters[jobId]; present {
			cmds[jobId] = pipe.HGet(jobStartTimePrefix+jobId, clusterId)
		}
	}

	_, e := pipe.Exec()
	if e != nil && e != redis.Nil {
		return runInfos, e
	}

	for jobId, cmd := range cmds {
		if cmd.Val() != "" {
			i, err := strconv.ParseInt(cmd.Val(), 10, 64)
			if err != nil {
				log.Errorf("Failed to parse start time for job %s because %s", jobId, err)
				continue
			}
			runInfos[jobId] = &RunInfo{
				StartTime:        time.Unix(0, i),
				CurrentClusterId: associatedClusters[jobId],
			}
		}
	}

	return runInfos, nil
}

func (repo *RedisJobRepository) GetQueueJobIds(queueName string) ([]string, error) {
	queuedIds, e := repo.db.ZRange(jobQueuePrefix+queueName, 0, -1).Result()
	return queuedIds, e
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

func (repo *RedisJobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {

	queuedIds, e := repo.db.ZRange(jobQueuePrefix+queue, 0, -1).Result()
	if e != nil {
		return nil, e
	}
	leasedIds, e := repo.db.ZRange(jobLeasedPrefix+queue, 0, -1).Result()
	if e != nil {
		return nil, e
	}

	jobSets := map[string]*api.JobSetInfo{}

	leasedJobs, e := repo.GetExistingJobsByIds(leasedIds)
	if e != nil {
		return nil, e
	}
	for _, job := range leasedJobs {
		info, ok := jobSets[job.JobSetId]
		if !ok {
			info = &api.JobSetInfo{Name: job.JobSetId}
			jobSets[job.JobSetId] = info
		}
		info.LeasedJobs++
	}

	queuedJobs, e := repo.GetExistingJobsByIds(queuedIds)
	if e != nil {
		return nil, e
	}
	for _, job := range queuedJobs {
		info, ok := jobSets[job.JobSetId]
		if !ok {
			info = &api.JobSetInfo{Name: job.JobSetId}
			jobSets[job.JobSetId] = info
		}
		info.QueuedJobs++
	}

	result := []*api.JobSetInfo{}
	for _, i := range jobSets {
		result = append(result, i)
	}
	return result, nil
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

func (repo *RedisJobRepository) AddRetryAttempt(jobId string) error {
	_, err := repo.db.Incr(jobRetriesPrefix + jobId).Result()
	return err
}

func (repo *RedisJobRepository) GetNumberOfRetryAttempts(jobId string) (int, error) {
	retriesStr, err := repo.db.Get(jobRetriesPrefix + jobId).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	retries, err := strconv.Atoi(retriesStr)
	if err != nil {
		return 0, err
	}

	return retries, nil
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

func (repo *RedisJobRepository) applyDefaults(spec *v1.PodSpec) {
	if spec != nil {
		for i := range spec.Containers {
			c := &spec.Containers[i]
			if c.Resources.Limits == nil {
				c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
			}
			if c.Resources.Requests == nil {
				c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
			}
			for k, v := range repo.defaultJobLimits {
				_, limitExists := c.Resources.Limits[v1.ResourceName(k)]
				_, requestExists := c.Resources.Limits[v1.ResourceName(k)]
				if !limitExists && !requestExists {
					c.Resources.Requests[v1.ResourceName(k)] = v
					c.Resources.Limits[v1.ResourceName(k)] = v
				}
			}
		}
	}
}

func addJob(db redis.Cmdable, job *api.Job, jobData *[]byte) *redis.Cmd {
	return addJobScript.Run(db,
		[]string{jobQueuePrefix + job.Queue, jobObjectPrefix + job.Id, jobSetPrefix + job.JobSetId, jobClientIdPrefix + job.Queue + keySeparator + job.ClientId},
		job.Id, job.Priority, *jobData, job.ClientId)
}

var addJobScript = redis.NewScript(`
local queueKey = KEYS[1]
local jobKey = KEYS[2]
local jobSetKey = KEYS[3]
local jobClientIdKey = KEYS[4]

local jobId = ARGV[1]
local jobPriority = ARGV[2]
local jobData = ARGV[3]
local clientId = ARGV[4]

if clientId ~= '' then
	local existingJobId = redis.call('GET', jobClientIdKey)
	if existingJobId then 
		return existingJobId
	end
	redis.call('SET', jobClientIdKey, jobId, 'EX', 14400)
end

redis.call('SET', jobKey, jobData)
redis.call('SADD', jobSetKey, jobId)
redis.call('ZADD', queueKey, jobPriority, jobId)

return jobId
`)

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
	return expireScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		jobId, float64(created.UnixNano()), float64(deadline.UnixNano()))
}

var expireScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]
local clusterAssociation = KEYS[3]

local jobId = ARGV[1]
local created = tonumber(ARGV[2])
local deadline = tonumber(ARGV[3])

local leasedTime = tonumber(redis.call('ZSCORE', leasedJobsSet, jobId))

if leasedTime ~= nil and leasedTime < deadline then
	redis.call('HDEL', clusterAssociation, jobId)
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
	redis.call('HDEL', clusterAssociation, jobId)
	local exists = redis.call('ZREM', leasedJobsSet, jobId)
	if exists ~= 0 then
		return redis.call('ZADD', queue, created, jobId)
	else
		return 0
	end
end
return 0
`)
