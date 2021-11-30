package repository

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

const jobObjectPrefix = "Job:"             // {jobId}            - job protobuf object
const jobStartTimePrefix = "Job:StartTime" // {jobId}            - map clusterId -> startTime
const jobQueuePrefix = "Job:Queue:"        // {queue}            - sorted set of jobIds by priority
const jobLeasedPrefix = "Job:Leased:"      // {queue}            - sorted set of jobIds by lease renewal time
const jobSetPrefix = "Job:Set:"            // {jobSetId}         - set of jobIds
const jobClusterMapKey = "Job:ClusterId"   //                    - map jobId -> cluster
const jobRetriesPrefix = "Job:Retries:"    // {jobId}            - number of retry attempts
const jobClientIdPrefix = "job:ClientId:"  // {queue}:{clientId} - corresponding jobId
const keySeparator = ":"

const queueResourcesBatchSize = 20000

const JobNotFound = "no job found with provided Id"

type UpdateJobResult struct {
	JobId string
	Job   *api.Job
	Error error
}

type JobRepository interface {
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	TryLeaseJobs(clusterId string, queue string, jobs []*api.Job) ([]*api.Job, error)
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
	UpdateStartTime(jobStartInfos []*JobStartInfo) ([]error, error)
	UpdateJobs(ids []string, mutator func([]*api.Job)) []UpdateJobResult
	GetJobRunInfos(jobIds []string) (map[string]*RunInfo, error)
	GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error)
	AddRetryAttempt(jobId string) error
	GetNumberOfRetryAttempts(jobId string) (int, error)
}

type RedisJobRepository struct {
	db              redis.UniversalClient
	retentionPolicy configuration.DatabaseRetentionPolicy
}

func NewRedisJobRepository(
	db redis.UniversalClient,
	retentionPolicy configuration.DatabaseRetentionPolicy) *RedisJobRepository {
	return &RedisJobRepository{db: db, retentionPolicy: retentionPolicy}
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

	returned, e := returnLease(repo.db, clusterId, job.Queue, job.Id, job.Priority).Int()
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
			deletionResult.setJobExpiryResult = pipe.Expire(jobObjectPrefix+job.Id, repo.retentionPolicy.JobRetentionDuration)
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

type JobStartInfo struct {
	JobId     string
	ClusterId string
	StartTime time.Time
}

func (repo *RedisJobRepository) UpdateStartTime(jobStartInfos []*JobStartInfo) ([]error, error) {
	jobErrors := make([]error, len(jobStartInfos), len(jobStartInfos))

	commands := make([]*redis.Cmd, len(jobStartInfos), len(jobStartInfos))
	pipe := repo.db.Pipeline()
	updateStartTimeScript.Load(pipe)

	for i, jobStartInfo := range jobStartInfos {
		commands[i] = updateStartTimeScript.Run(
			pipe,
			[]string{
				jobStartTimePrefix + jobStartInfo.JobId,
				jobClusterMapKey,
				jobObjectPrefix + jobStartInfo.JobId,
			},
			jobStartInfo.ClusterId,
			jobStartInfo.StartTime.UTC().UnixNano())
	}

	_, err := pipe.Exec()
	if err != nil {
		return nil, fmt.Errorf("update start time multiple: %v", err)
	}

	for i, cmd := range commands {
		err := cmd.Err()
		if err != nil {
			jobErrors[i] = fmt.Errorf("error updating job start time in redis: %v", err)
		} else {
			jobErrors[i] = nil
		}
		ret, err := cmd.Int()
		if err != nil {
			jobErrors[i] = fmt.Errorf("error parsing result from redis: %v", err)
		}
		if ret == updateStartTimeJobNotFound {
			jobErrors[i] = fmt.Errorf(JobNotFound)
		}
	}

	return jobErrors, nil
}

const updateStartTimeJobNotFound = -3

var updateStartTimeScript = redis.NewScript(fmt.Sprintf(`
local startTimeKey = KEYS[1]
local clusterAssociation = KEYS[2]
local job = KEYS[3]

local clusterId = ARGV[1]
local startTime = ARGV[2]
local startTimeNumber = tonumber(ARGV[2])

local ttl = redis.call('TTL', job)
local existsAndNotExpired = ttl == -1
if not existsAndNotExpired then
	return %d
end

local currentStartTime = tonumber(redis.call('HGET', startTimeKey, clusterId))

if currentStartTime ~= nil and currentStartTime < startTimeNumber then
	return 0
end

return redis.call('HSET', startTimeKey, clusterId, startTime)
`, updateStartTimeJobNotFound))

func (repo *RedisJobRepository) UpdateJobs(ids []string, mutator func([]*api.Job)) []UpdateJobResult {
	return repo.updateJobs(ids, mutator, 250, 3, 100*time.Millisecond)
}

func (repo *RedisJobRepository) updateJobs(ids []string, mutator func([]*api.Job), batchSize int, retries int, retryDelay time.Duration) []UpdateJobResult {
	batchedIds := util.Batch(ids, batchSize)
	result := []UpdateJobResult{}

	for _, batch := range batchedIds {
		batchResult, err := repo.updateJobBatchWithRetry(batch, mutator, retries, retryDelay)
		if err == nil {
			for _, jobResult := range batchResult {
				result = append(result, jobResult)
			}
		} else {
			for _, id := range batch {
				result = append(result, UpdateJobResult{JobId: id, Job: nil, Error: err})
			}
		}
	}
	return result
}

func (repo *RedisJobRepository) updateJobBatchWithRetry(ids []string, mutator func([]*api.Job), retries int, retryDelay time.Duration) ([]UpdateJobResult, error) {
	for retry := 0; ; retry++ {
		result, err := repo.updateJobBatch(ids, mutator)
		if err != redis.TxFailedErr {
			return result, err
		}
		log.Warnf("UpdateJobs: Redis Transaction failed (job ids %s)", strings.Join(ids, ", "))

		if retry >= retries {
			log.Warnf("UpdateJobs: Redis Transaction failed after retrying, giving up (job ids %s)", strings.Join(ids, ", "))
			return nil, redis.TxFailedErr
		}
		time.Sleep(retryDelay)
	}
}

func (repo *RedisJobRepository) updateJobBatch(ids []string, mutator func([]*api.Job)) ([]UpdateJobResult, error) {

	var keysToWatch []string
	for _, id := range ids {
		keysToWatch = append(keysToWatch, jobObjectPrefix+id)
	}

	result := []UpdateJobResult{}
	err := repo.db.Watch(func(tx *redis.Tx) error {

		// There is currently no clean way to implement the WATCH/GET/MULTI/SET/EXEC pattern with go-redis
		// because Watch() calls both WATCH and MULTI together.
		// To work round this, GetExistingJobsByIds is delberately using a separate Redis connection, not tx.Pipeline().
		jobs, err := repo.GetExistingJobsByIds(ids)
		if err != nil {
			return err
		}

		mutator(jobs)

		jobDatas := make([][]byte, len(jobs))
		for i, job := range jobs {
			jobData, err := proto.Marshal(job)
			if err != nil {
				return err
			}
			jobDatas[i] = jobData
		}

		commands := make([]*redis.Cmd, len(jobs))
		pipe := tx.Pipeline()
		updateJobAndPriorityScript.Load(pipe)

		for i, job := range jobs {
			commands[i] = updateJobAndPriority(pipe, job, job.Priority, &jobDatas[i])
		}
		_, err = pipe.Exec()
		if err != nil {
			return err
		}

		for i, cmd := range commands {
			err := cmd.Err()
			if err != nil {
				log.Warnf("UpdateJobs: Failed to update job %s: %v", jobs[i].Id, err)
				result = append(result, UpdateJobResult{JobId: jobs[i].Id, Job: nil, Error: err})
			} else {
				result = append(result, UpdateJobResult{JobId: jobs[i].Id, Job: jobs[i], Error: nil})
			}
		}

		return nil
	}, keysToWatch...)

	if err != nil {
		return nil, err
	}

	return result, nil
}

func updateJobAndPriority(db redis.Cmdable, job *api.Job, newPriority float64, jobData *[]byte) *redis.Cmd {
	return updateJobAndPriorityScript.Run(db,
		[]string{jobQueuePrefix + job.Queue, jobObjectPrefix + job.Id},
		job.Id, newPriority, *jobData)
}

// If the job key has a defined TTL, it implies that the job has finished and updating is irrelevant
var updateJobAndPriorityScript = redis.NewScript(`
local queue = KEYS[1]
local job = KEYS[2]

local jobId = ARGV[1]
local newPriority = ARGV[2]
local jobData = ARGV[3]

local exists = redis.call('GET', job)
local existsQueued = redis.call('ZSCORE', queue, jobId)

if exists then
	local ttl = redis.call('TTL', job)
	if ttl < 0 then
		redis.call('SET', job, jobData)
	end
end

if existsQueued then
	redis.call('ZADD', queue, newPriority, jobId)
end

return 0
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

	tx := repo.db.TxPipeline()
	queuedIdsCommand := tx.ZRange(jobQueuePrefix+queue, 0, -1)
	leasedIdsCommand := tx.ZRange(jobLeasedPrefix+queue, 0, -1)
	jobSetIdsCommand := tx.SMembers(jobSetPrefix + jobSetId)
	_, _ = tx.Exec()

	queuedIds, e := queuedIdsCommand.Result()
	if e != nil {
		return nil, e
	}
	leasedIds, e := leasedIdsCommand.Result()
	if e != nil {
		return nil, e
	}
	jobSetIds, e := jobSetIdsCommand.Result()
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

	tx := repo.db.TxPipeline()
	queuedIdsCommand := tx.ZRange(jobQueuePrefix+queue, 0, -1)
	leasedIdsCommand := tx.ZRange(jobLeasedPrefix+queue, 0, -1)
	_, _ = tx.Exec()

	queuedIds, e := queuedIdsCommand.Result()
	if e != nil {
		return nil, e
	}
	leasedIds, e := leasedIdsCommand.Result()
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
		cmds[job] = expire(pipe, job.Queue, job.Id, job.Priority, deadline)
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

func expire(db redis.Cmdable, queueName string, jobId string, priority float64, deadline time.Time) *redis.Cmd {
	return expireScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		jobId, priority, float64(deadline.UnixNano()))
}

var expireScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]
local clusterAssociation = KEYS[3]

local jobId = ARGV[1]
local priority = tonumber(ARGV[2])
local deadline = tonumber(ARGV[3])

local leasedTime = tonumber(redis.call('ZSCORE', leasedJobsSet, jobId))

if leasedTime ~= nil and leasedTime < deadline then
	redis.call('HDEL', clusterAssociation, jobId)
	local exists = redis.call('ZREM', leasedJobsSet, jobId)
	if exists ~= 0 then
		return redis.call('ZADD', queue, priority, jobId)
	else
		return 0
	end
end
`)

func returnLease(db redis.Cmdable, clusterId string, queueName string, jobId string, priority float64) *redis.Cmd {
	return returnLeaseScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		clusterId, jobId, priority)
}

var returnLeaseScript = redis.NewScript(`
local queue = KEYS[1]
local leasedJobsSet = KEYS[2]
local clusterAssociation = KEYS[3]

local clusterId = ARGV[1]
local jobId = ARGV[2]
local priority = tonumber(ARGV[3])

local currentClusterId = redis.call('HGET', clusterAssociation, jobId)

if currentClusterId == clusterId then
	redis.call('HDEL', clusterAssociation, jobId)
	local exists = redis.call('ZREM', leasedJobsSet, jobId)
	if exists ~= 0 then
		return redis.call('ZADD', queue, priority, jobId)
	else
		return 0
	end
end
return 0
`)
