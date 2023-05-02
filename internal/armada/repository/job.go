package repository

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

const (
	jobObjectPrefix    = "Job:"          // {jobId}            - job protobuf object
	jobStartTimePrefix = "Job:StartTime" // {jobId}            - map clusterId -> startTime
	jobQueuePrefix     = "Job:Queue:"    // {queue}            - sorted set of jobIds by priority
	jobLeasedPrefix    = "Job:Leased:"   // {queue}            - sorted set of jobIds by lease renewal time
	jobSetPrefix       = "Job:Set:"      // {jobSetId}         - set of jobIds
	jobClusterMapKey   = "Job:ClusterId" //                    - map jobId -> cluster
	jobRetriesPrefix   = "Job:Retries:"  // {jobId}            - number of retry attempts
	jobClientIdPrefix  = "job:ClientId:" // {queue}:{clientId} - corresponding jobId
	jobExistsPrefix    = "Job:added"     // {jobId}            - flag to say we've added the job
	keySeparator       = ":"
	pulsarJobPrefix    = "PulsarJob:" // {jobId}            - pulsarjob protobuf object
)

type ErrJobNotFound struct {
	JobId     string
	ClusterId string
}

func (err *ErrJobNotFound) Error() string {
	return fmt.Sprintf("could not find job with ID %q assigned to cluster %q", err.JobId, err.ClusterId)
}

type UpdateJobResult struct {
	JobId string
	Job   *api.Job
	Error error
}

// JobResult is used by GetJobsByIds to bundle a job with any error that occurred
// when getting the job.
type JobResult struct {
	JobId string
	Job   *api.Job
	Error error
}

type JobRepository interface {
	PeekQueue(queue string, limit int64) ([]*api.Job, error)
	// TryLeaseJobs attempts to lease a set of jobs to the executor with the given clusterId.
	// Takes as argument a map from queue name to slice of job ids to lease from that queue.
	// Returns a map from queue name to ids of successfully leased jobs for that queue.
	TryLeaseJobs(clusterId string, jobIdsByQueue map[string][]string) (map[string][]string, error)
	AddJobs(job []*api.Job) ([]*SubmitJobResult, error)
	GetJobsByIds(ids []string) ([]*JobResult, error)
	GetExistingJobsByIds(ids []string) ([]*api.Job, error)
	FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error)
	GetQueueSizes(queues []*api.Queue) (sizes []int64, e error)
	GetQueueJobIds(queueName string) ([]string, error)
	RenewLease(clusterId string, jobIds []string) (renewed []string, e error)
	ExpireLeases(queue string, deadline time.Time) (expired []*api.Job, e error)
	ExpireLeasesById(jobIds []string, deadline time.Time) (expired []*api.Job, e error)
	ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error)
	DeleteJobs(jobs []*api.Job) (map[*api.Job]error, error)
	GetActiveJobIds(queue string, jobSetId string) ([]string, error)
	GetJobSetJobIds(queue string, jobSetId string, filter *JobSetFilter) ([]string, error)
	GetLeasedJobIds(queue string) ([]string, error)
	UpdateStartTime(jobStartInfos []*JobStartInfo) ([]error, error)
	UpdateJobs(ids []string, mutator func([]*api.Job)) ([]UpdateJobResult, error)
	GetJobRunInfos(jobIds []string) (map[string]*RunInfo, error)
	GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error)
	AddRetryAttempt(jobId string) error
	GetNumberOfRetryAttempts(jobId string) (int, error)
	StorePulsarSchedulerJobDetails(jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error
	GetPulsarSchedulerJobDetails(jobIds string) (*schedulerobjects.PulsarSchedulerJobDetails, error)
	DeletePulsarSchedulerJobDetails(jobId []string) error
}

type RedisJobRepository struct {
	db redis.UniversalClient
}

func NewRedisJobRepository(
	db redis.UniversalClient,
) *RedisJobRepository {
	return &RedisJobRepository{db: db}
}

// TODO DuplicateDetected should be remove in favour of setting the error to
// indicate the job already exists (e.g., by creating ErrJobExists).
type SubmitJobResult struct {
	JobId             string
	SubmittedJob      *api.Job
	AlreadyProcessed  bool
	DuplicateDetected bool
	Error             error
}

func (repo *RedisJobRepository) AddJobs(jobs []*api.Job) ([]*SubmitJobResult, error) {
	pipe := repo.db.Pipeline()
	addJobScript.Load(pipe)

	saveResults := make([]*redis.Cmd, 0, len(jobs))
	for _, job := range jobs {
		jobData, err := proto.Marshal(job)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		result := addJob(pipe, job, &jobData)
		saveResults = append(saveResults, result)
	}

	_, err := pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	result := make([]*SubmitJobResult, 0, len(jobs))
	for i, saveResult := range saveResults {
		resultJobId, err := saveResult.String()
		alreadyProcessed := resultJobId == "-1"
		duplicatedDetected := !alreadyProcessed && resultJobId != jobs[i].Id
		submitJobResult := &SubmitJobResult{
			JobId:             resultJobId,
			SubmittedJob:      jobs[i],
			Error:             err,
			DuplicateDetected: duplicatedDetected,
			AlreadyProcessed:  alreadyProcessed,
		}
		result = append(result, submitJobResult)
	}
	return result, nil
}

func (repo *RedisJobRepository) RenewLease(clusterId string, jobIds []string) (renewedJobIds []string, e error) {
	// TODO: If we can pass in the queue, we don't need to load jobs from Redis.
	jobs, err := repo.GetExistingJobsByIds(jobIds)
	if err != nil {
		return nil, err
	}
	jobsById := make(map[string]*api.Job, len(jobIds))
	for _, job := range jobs {
		jobsById[job.Id] = job
	}
	jobIdsByQueue := make(map[string][]string)
	for _, job := range jobs {
		jobIdsByQueue[job.Queue] = append(jobIdsByQueue[job.Queue], job.Id)
	}
	leasedJobIdsByQueue, err := repo.leaseJobs(clusterId, jobIdsByQueue)
	if err != nil {
		return nil, err
	}
	leasedJobIds := make([]string, 0, len(jobs))
	for _, jobIds := range leasedJobIdsByQueue {
		leasedJobIds = append(leasedJobIds, jobIds...)
	}
	return leasedJobIds, nil
}

func (repo *RedisJobRepository) ReturnLease(clusterId string, jobId string) (returnedJob *api.Job, err error) {
	jobs, err := repo.GetExistingJobsByIds([]string{jobId})
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		// Job has already been deleted; no more changes necessary.
		return nil, nil
	} else if len(jobs) != 1 {
		err = fmt.Errorf("expected to get exactly 0 or 1 job, but got %d jobs", len(jobs))
		return nil, errors.WithStack(err)
	}
	job := jobs[0]
	returned, err := returnLease(repo.db, clusterId, job.Queue, job.Id, job.Priority).Int()
	if err != nil {
		err = errors.WithMessagef(err, "error returning lease for job %s and cluster %s", job.Id, clusterId)
		return nil, err
	}
	if returned > 0 {
		return job, nil
	}
	return nil, nil
}

type deleteJobRedisResponse struct {
	job                            *api.Job
	removeFromLeasedResult         *redis.IntCmd
	removeFromQueueResult          *redis.IntCmd
	removeClusterAssociationResult *redis.IntCmd
	removeStartTimeResult          *redis.IntCmd
	deleteJobSetIndexResult        *redis.IntCmd
	deleteJobRetriesResult         *redis.IntCmd
	deleteJobObjectResult          *redis.IntCmd
}

func (repo *RedisJobRepository) DeleteJobs(jobs []*api.Job) (map[*api.Job]error, error) {
	pipe := repo.db.TxPipeline()
	deletionResults := make([]*deleteJobRedisResponse, 0, len(jobs))
	for _, job := range jobs {
		// This is safe because attempting to delete non-existing keys results in a no-op.
		deletionResult := &deleteJobRedisResponse{job: job}
		deletionResult.removeFromQueueResult = pipe.ZRem(jobQueuePrefix+job.Queue, job.Id)
		deletionResult.removeFromLeasedResult = pipe.ZRem(jobLeasedPrefix+job.Queue, job.Id)
		deletionResult.removeClusterAssociationResult = pipe.HDel(jobClusterMapKey, job.Id)
		deletionResult.removeStartTimeResult = pipe.Del(jobStartTimePrefix + job.Id)
		deletionResult.deleteJobSetIndexResult = pipe.SRem(jobSetPrefix+job.JobSetId, job.Id)
		deletionResult.deleteJobRetriesResult = pipe.Del(jobRetriesPrefix + job.Id)
		deletionResult.deleteJobObjectResult = pipe.Del(jobObjectPrefix + job.Id)

		// Don't care if deletion fails during compatibility period
		pipe.SRem(jobSetPrefix+job.Queue+keySeparator+job.JobSetId, job.Id)

		deletionResults = append(deletionResults, deletionResult)
	}
	if _, err := pipe.Exec(); err != nil {
		return nil, errors.WithStack(err)
	}

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
	return cancelledJobs, nil
}

func processDeletionResponse(deletionResponse *deleteJobRedisResponse) (int64, error) {
	var totalUpdates int64 = 0
	var result *multierror.Error

	modified, err := deletionResponse.removeFromLeasedResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.removeFromQueueResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.deleteJobSetIndexResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.removeClusterAssociationResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.removeStartTimeResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.deleteJobRetriesResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	modified, err = deletionResponse.deleteJobObjectResult.Result()
	totalUpdates += modified
	result = multierror.Append(result, err)

	return totalUpdates, result.ErrorOrNil()
}

// PeekQueue returns the highest-priority jobs in the given queue.
// At most limits jobs are returned.
func (repo *RedisJobRepository) PeekQueue(queue string, limit int64) ([]*api.Job, error) {
	ids, err := repo.db.ZRange(jobQueuePrefix+queue, 0, limit-1).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	jobs, err := repo.GetExistingJobsByIds(ids)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

// TryLeaseJobs attempts to assign jobs to a given cluster and returns a list composed of the jobs
// that were successfully leased.
func (repo *RedisJobRepository) TryLeaseJobs(clusterId string, jobIdsByQueue map[string][]string) (map[string][]string, error) {
	leasedJobIdsByQueue, err := repo.leaseJobs(clusterId, jobIdsByQueue)
	if err != nil {
		return nil, err
	}
	return leasedJobIdsByQueue, nil
}

// GetExistingJobsByIds queries Redis for job details. Missing jobs are omitted, i.e.,
// the returned list may be shorter than the provided list of IDs.
func (repo *RedisJobRepository) GetExistingJobsByIds(ids []string) ([]*api.Job, error) {
	jobResults, err := repo.GetJobsByIds(ids)
	if err != nil {
		return nil, err
	}
	var result *multierror.Error
	jobs := make([]*api.Job, 0, len(jobResults))
	for _, jobResult := range jobResults {
		var errJobNotFound *ErrJobNotFound
		var errNotFound *armadaerrors.ErrNotFound
		if errors.As(jobResult.Error, &errJobNotFound) || errors.As(jobResult.Error, &errNotFound) {
			continue
		} else if jobResult.Error != nil {
			err = errors.WithMessagef(jobResult.Error, "error getting job with id %s from database", jobResult.JobId)
			result = multierror.Append(result, err)
			continue
		}
		// Ensure job.GetAnnotations and podSpec.NodeSelector are initialised.
		// Necessary to mutate these in-place during scheduling.
		if jobResult.Job.Annotations == nil {
			jobResult.Job.Annotations = make(map[string]string)
		}
		if jobResult.Job.PodSpec != nil && jobResult.Job.PodSpec.NodeSelector == nil {
			jobResult.Job.PodSpec.NodeSelector = make(map[string]string)
		}
		for _, podSpec := range jobResult.Job.PodSpecs {
			if podSpec != nil && podSpec.NodeSelector == nil {
				jobResult.Job.PodSpec.NodeSelector = make(map[string]string)
			}
		}
		jobs = append(jobs, jobResult.Job)
	}
	return jobs, result.ErrorOrNil()
}

// GetJobsByIds attempts to get all requested jobs from the database.
// Any error in getting a job is set to the Err field of the corresponding JobResult.
func (repo *RedisJobRepository) GetJobsByIds(ids []string) ([]*JobResult, error) {
	pipe := repo.db.Pipeline()
	var cmds []*redis.StringCmd
	for _, id := range ids {
		cmds = append(cmds, pipe.Get(jobObjectPrefix+id))
	}

	_, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		return nil, errors.WithStack(err)
	}

	var results []*JobResult
	for index, cmd := range cmds {
		result := &JobResult{JobId: ids[index]}
		results = append(results, result)

		_, err := cmd.Result()
		if err == redis.Nil {
			result.Error = &armadaerrors.ErrNotFound{
				Type:  "job",
				Value: ids[index],
			}
			continue
		} else if err != nil {
			result.Error = errors.WithMessagef(err, "job id %s", ids[index])
			continue
		}

		d, _ := cmd.Bytes() // we already checked the error above
		result.Job = &api.Job{}
		err = proto.Unmarshal(d, result.Job)
		if err != nil {
			err = errors.WithMessagef(err, "job id %s", ids[index])
			return nil, errors.WithStack(err)
		}

		// TODO This shouldn't be here. We write these when creating the job,
		// and the getter shouldn't mutate the object read from the database.
		for _, podSpec := range result.Job.GetAllPodSpecs() {
			// TODO: remove, RequiredNodeLabels is deprecated and will be removed in future versions
			for k, v := range result.Job.RequiredNodeLabels {
				if podSpec.NodeSelector == nil {
					podSpec.NodeSelector = map[string]string{}
				}
				podSpec.NodeSelector[k] = v
			}
		}
	}

	return results, nil
}

func (repo *RedisJobRepository) FilterActiveQueues(queues []*api.Queue) ([]*api.Queue, error) {
	pipe := repo.db.Pipeline()
	cmds := make(map[*api.Queue]*redis.IntCmd)
	for _, queue := range queues {
		// empty (even sorted) sets gets deleted by redis automatically
		cmds[queue] = pipe.Exists(jobQueuePrefix + queue.Name)
	}
	_, err := pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
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
	_, err = pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sizes = []int64{}
	for _, cmd := range cmds {
		sizes = append(sizes, cmd.Val())
	}
	return sizes, nil
}

func (repo *RedisJobRepository) GetLeasedJobIds(queue string) ([]string, error) {
	val, err := repo.db.ZRange(jobLeasedPrefix+queue, 0, -1).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return val, nil
}

func (repo *RedisJobRepository) getAssociatedCluster(jobIds []string) (map[string]string, error) {
	associatedCluster := make(map[string]string, len(jobIds))
	pipe := repo.db.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(jobIds))

	for _, jobId := range jobIds {
		cmds[jobId] = pipe.HGet(jobClusterMapKey, jobId)
	}

	_, err := pipe.Exec()
	if err != nil && err != redis.Nil {
		return nil, errors.WithStack(err)
	}

	for jobId, cmd := range cmds {
		clusterId, err := cmd.Result()
		if err != nil && err != redis.Nil {
			err = errors.WithMessagef(err, "job id %s", jobId)
			return nil, errors.WithStack(err)
		}
		if clusterId != "" {
			associatedCluster[jobId] = cmd.Val()
		}
	}

	return associatedCluster, nil
}

type JobStartInfo struct {
	// Unique ID assigned to each job.
	JobId string
	// Name of the cluster (as specified in the executor config) the job is assigned to.
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
			jobStartInfo.StartTime.UTC().UnixNano(),
		)
	}

	// TODO If a command queued to a pipeline errors, pipe.Exec() returns the error returned by the
	// first command to return an error. Hence, if we're pipelining commands, jobErrors will either
	// be nil or contain only nil (in the case of no errors).
	_, err := pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for i, command := range commands {
		// For each job, store errors resulting from attempting to run the script.
		// Note that the script may have be started successfully (i.e., the err here is nil),
		// but the script may have returned a non-zero exit code, indicating something went
		// wrong inside the script.
		if err := command.Err(); err != nil {
			err = errors.WithMessagef(err, "error updating start time for job with id %s", jobStartInfos[i].JobId)
			err = errors.WithStack(err)
			jobErrors[i] = err
		}

		// Check the return code of the script
		// The error returned by cmd.Int() is equal to the above err if running the script failed.
		// If the script ran, but produced an exit code that could not be parsed as an integer,
		// the err returned by cmd.Int() is non-nil.
		code, err := command.Int()
		if err != nil && err != jobErrors[i] {
			return nil, errors.Wrapf(err, "exit code %d", code)
		}
		if err == nil && code == updateStartTimeJobNotFound {
			jobStartInfo := jobStartInfos[i]
			jobErrors[i] = &ErrJobNotFound{JobId: jobStartInfo.JobId, ClusterId: jobStartInfo.ClusterId}
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

// TODO Redis supports setting a retry parameter. Why do we re-implement that functionality?
func (repo *RedisJobRepository) UpdateJobs(ids []string, mutator func([]*api.Job)) ([]UpdateJobResult, error) {
	return repo.updateJobs(ids, mutator, 250, 3, 100*time.Millisecond), nil
}

// TODO: This function should return a multierror
func (repo *RedisJobRepository) updateJobs(ids []string, mutator func([]*api.Job), batchSize int, retries int, retryDelay time.Duration) []UpdateJobResult {
	batchedIds := util.Batch(ids, batchSize)
	result := make([]UpdateJobResult, 0, len(ids))

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

// updateJobBatch reads jobs from Redis, applies mutator separately for each job, and writes the
// updated jobs back to Redis. This process is performed in an optimistic lock, such that the
// updated jobs are written back to Redis only if none of the jobs were changed in Redis between
// being read and written back.
//
// Any jobs that can't be found are ignored.
//
// For this reason, mutator may not read from any additional keys in Redis, since those keys
// would not be covered by the optimistic lock.
//
// This process is attempted up to maxRetries times and each attempt is separated by retryDelay.
func (repo *RedisJobRepository) updateJobBatchWithRetry(
	ids []string,
	mutator func([]*api.Job),
	maxRetries int,
	retryDelay time.Duration,
) ([]UpdateJobResult, error) {
	// Redis supports transactions via optimistic locking using the WATCH/READ/SET pattern
	// First, we mark all keys that the operation depends on
	// Hence, keysToWatch must contain all keys read from inside txf (see below)
	var keysToWatch []string
	for _, id := range ids {
		keysToWatch = append(keysToWatch, jobObjectPrefix+id)
	}

	// Transactional function
	result := make([]UpdateJobResult, 0, len(ids))
	txf := func(tx *redis.Tx) error {
		// Read all data the operation depends on
		// All keys read by GetExistingJobsByIds must be added to keysToWatch
		jobs, err := repo.GetExistingJobsByIds(ids)
		if err != nil {
			return err
		}

		// Operation to run (locally in optimistic lock)
		mutator(jobs)

		// Marshal the resulting jobs in preparation for writing back to Redis
		jobDatas := make([][]byte, len(jobs))
		for i, job := range jobs {
			jobData, err := proto.Marshal(job)
			if err != nil {
				return errors.Wrapf(err, "job id %s", job.Id)
			}
			jobDatas[i] = jobData
		}

		// Write to Redis
		// The watched keys are unwatched upon calling exec.
		// For this reason, we must use a TxPipe, which guarantees that the operations added
		// to the pipe are performed in sequence without being interleaved with other concurrent
		// operations. If we use a regular pipe, there is a race condition, where another client
		// mutates a value this function depends on after we have unwatched it, but before we have
		// written out results back to Redis.
		commands := make([]*redis.Cmd, len(jobs))
		pipe := tx.TxPipeline()
		updateJobAndPriorityScript.Load(pipe)
		for i, job := range jobs {
			newPriority := job.Priority
			jobData := &jobDatas[i]
			commands[i] = updateJobAndPriorityScript.Run(
				pipe,
				[]string{jobQueuePrefix + job.Queue, jobObjectPrefix + job.Id},
				job.Id, newPriority, *jobData,
			)
		}

		// TODO We append to results even if an error occurs. However, we only return results if
		// exec doesn't return an error. Because exec returns error in the commands it executes,
		// this means that we results isn't returned if any single command errors.
		//
		// Returns redis.TxFailedErr if we lost the optimistic lock.
		_, err = pipe.Exec()
		if err == redis.TxFailedErr { // Indicates we lost the optimistic lock
			return err // Return without stack to ensure retries happen correctly
		} else if err != nil {
			return errors.WithStack(err)
		}

		for i, cmd := range commands {
			err := cmd.Err()
			if err != nil {
				log.Warnf("[RedisJobRepository.updateJobBatch]: error updating job %s: %s", jobs[i].Id, err)
				result = append(result, UpdateJobResult{JobId: jobs[i].Id, Job: nil, Error: err})
			} else {
				result = append(result, UpdateJobResult{JobId: jobs[i].Id, Job: jobs[i], Error: nil})
			}
		}

		return nil
	}

	// Run txf under optimistic lock on keys in keysToWatch
	for retries := 0; retries < maxRetries; retries++ {
		err := repo.db.Watch(txf, keysToWatch...)
		if err == nil {
			// Success.
			return result, nil
		}
		if err == redis.TxFailedErr {
			// Optimistic lock lost. Retry.
			time.Sleep(retryDelay)
			continue
		}
		// Return any other error.
		return nil, err
	}

	// TODO I think we should return a more informative error message instead of logging a warning
	// and returning a Redis error. I leave it for now since tests depend on getting a redis.TxFailedErr
	log.Warnf("[RedisJobRepository.updateJobBatchWithRetry]: Redis Transaction failed after retrying, giving up (job ids %s)", strings.Join(ids, ", "))
	return nil, redis.TxFailedErr
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

// GetJobRunInfos returns run info for the cluster that each of the provided jobs is leased to.
// Jobs not leased to any cluster or that does not have a start time are omitted.
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

	_, err = pipe.Exec()
	if err != nil && err != redis.Nil {
		return runInfos, errors.WithStack(err)
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
	queuedIds, err := repo.db.ZRange(jobQueuePrefix+queueName, 0, -1).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return queuedIds, nil
}

func (repo *RedisJobRepository) GetActiveJobIds(queue string, jobSetId string) ([]string, error) {
	return repo.GetJobSetJobIds(queue, jobSetId, &JobSetFilter{
		IncludeLeased: true,
		IncludeQueued: true,
	})
}

type JobSetFilter struct {
	IncludeQueued bool
	IncludeLeased bool
}

func (repo *RedisJobRepository) GetJobSetJobIds(queue string, jobSetId string, filter *JobSetFilter) ([]string, error) {
	var queuedIdsCommandResult *redis.StringSliceCmd
	var leasedIdsCommandResult *redis.StringSliceCmd

	tx := repo.db.TxPipeline()
	if filter == nil || filter.IncludeQueued {
		queuedIdsCommandResult = tx.ZRange(jobQueuePrefix+queue, 0, -1)
	}
	if filter == nil || filter.IncludeLeased {
		leasedIdsCommandResult = tx.ZRange(jobLeasedPrefix+queue, 0, -1)
	}
	jobSetIdsCommand := tx.SMembers(jobSetPrefix + jobSetId)

	_, err := tx.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	activeJobIds := []string{}
	if filter == nil || filter.IncludeQueued {
		queuedIds, err := queuedIdsCommandResult.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		activeJobIds = append(activeJobIds, queuedIds...)
	}
	if filter == nil || filter.IncludeLeased {
		leasedIds, err := leasedIdsCommandResult.Result()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		activeJobIds = append(activeJobIds, leasedIds...)
	}

	jobSetIds, err := jobSetIdsCommand.Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	activeIds := util.StringListToSet(activeJobIds)
	activeJobSetIds := []string{}
	for _, id := range jobSetIds {
		if activeIds[id] {
			activeJobSetIds = append(activeJobSetIds, id)
		}
	}
	return activeJobSetIds, nil
}

// GetQueueActiveJobSets returns a list of length equal to the number of unique job sets
// in the given queue, where each element contains the number of queued and leased jobs
// that are part of that job set.
func (repo *RedisJobRepository) GetQueueActiveJobSets(queue string) ([]*api.JobSetInfo, error) {
	tx := repo.db.TxPipeline()
	queuedIdsCommand := tx.ZRange(jobQueuePrefix+queue, 0, -1)
	leasedIdsCommand := tx.ZRange(jobLeasedPrefix+queue, 0, -1)

	// If there's an error internal to Exec, an error is returned
	// If any of the commands submitted to exec errors, the first error is returned
	_, err := tx.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	queuedIds, err := queuedIdsCommand.Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	leasedIds, err := leasedIdsCommand.Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Maps job set IDs to objects containing the number of queued and leased jobs for each job set
	jobSets := map[string]*api.JobSetInfo{}

	// Count number of leased jobs
	leasedJobs, err := repo.GetExistingJobsByIds(leasedIds)
	if err != nil {
		return nil, err
	}
	for _, job := range leasedJobs {
		info, ok := jobSets[job.JobSetId]
		if !ok {
			info = &api.JobSetInfo{Name: job.JobSetId}
			jobSets[job.JobSetId] = info
		}
		info.LeasedJobs++
	}

	// Count number of queued jobs
	queuedJobs, err := repo.GetExistingJobsByIds(queuedIds)
	if err != nil {
		return nil, err
	}
	for _, job := range queuedJobs {
		info, ok := jobSets[job.JobSetId]
		if !ok {
			info = &api.JobSetInfo{Name: job.JobSetId}
			jobSets[job.JobSetId] = info
		}
		info.QueuedJobs++
	}

	// Flatten the map
	result := []*api.JobSetInfo{}
	for _, i := range jobSets {
		result = append(result, i)
	}

	return result, nil
}

// ExpireLeases expires the leases on all jobs for the provided queue.
func (repo *RedisJobRepository) ExpireLeases(queue string, deadline time.Time) ([]*api.Job, error) {
	maxScore := strconv.FormatInt(deadline.UnixNano(), 10)

	// TODO: expire just limited number here ???
	ids, err := repo.db.ZRangeByScore(jobLeasedPrefix+queue, redis.ZRangeBy{Max: maxScore, Min: "-Inf"}).Result()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return repo.ExpireLeasesById(ids, deadline)
}

func (repo *RedisJobRepository) ExpireLeasesById(jobIds []string, deadline time.Time) ([]*api.Job, error) {
	if len(jobIds) == 0 {
		return make([]*api.Job, 0), nil
	}

	expiringJobs, err := repo.GetExistingJobsByIds(jobIds)
	if err != nil {
		return nil, err
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
	_, err = pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for job, cmd := range cmds {
		value, err := cmd.Int()
		if err != nil {
			// This happens if the job is missing or if the return code can't be parsed to an int
			err = errors.Wrapf(err, "error getting script return code: %d", value)
			log.Error(err)
		} else if value > 0 {
			expired = append(expired, job)
		}
	}
	return expired, nil
}

func (repo *RedisJobRepository) AddRetryAttempt(jobId string) error {
	_, err := repo.db.Incr(jobRetriesPrefix + jobId).Result()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (repo *RedisJobRepository) GetNumberOfRetryAttempts(jobId string) (int, error) {
	retriesStr, err := repo.db.Get(jobRetriesPrefix + jobId).Result()
	if err == redis.Nil {
		return 0, nil
	} else if err != nil {
		return 0, errors.WithStack(err)
	}

	retries, err := strconv.Atoi(retriesStr)
	if err != nil {
		return 0, errors.WithStack(err)
	}

	return retries, nil
}

func (repo *RedisJobRepository) StorePulsarSchedulerJobDetails(jobDetails []*schedulerobjects.PulsarSchedulerJobDetails) error {
	pipe := repo.db.Pipeline()
	for _, job := range jobDetails {
		key := fmt.Sprintf("%s%s", pulsarJobPrefix, job.JobId)
		jobData, err := proto.Marshal(job)
		if err != nil {
			return errors.WithStack(err)
		}
		pipe.Set(key, jobData, 375*24*time.Hour) // expire after a year
	}
	_, err := pipe.Exec()
	if err != nil {
		return errors.Wrapf(err, "error storing pulsar job details in redis")
	}
	return nil
}

func (repo *RedisJobRepository) GetPulsarSchedulerJobDetails(jobId string) (*schedulerobjects.PulsarSchedulerJobDetails, error) {
	cmd := repo.db.Get(pulsarJobPrefix + jobId)

	bytes, err := cmd.Bytes()
	if err != nil && err != redis.Nil {
		return nil, errors.Wrapf(err, "Errror retrieving job details for %s in redis", jobId)
	}
	if err == redis.Nil {
		return nil, nil
	}
	details, err := protoutil.Unmarshall(bytes, &schedulerobjects.PulsarSchedulerJobDetails{})
	if err != nil {
		return nil, errors.Wrapf(err, "Errror unmarshalling job details for %s in redis", jobId)
	}

	return details, nil
}

func (repo *RedisJobRepository) DeletePulsarSchedulerJobDetails(jobIds []string) error {
	pipe := repo.db.Pipeline()
	for _, jobId := range jobIds {
		pipe.Del(pulsarJobPrefix + jobId)
	}
	if _, err := pipe.Exec(); err != nil {
		return errors.Wrap(err, "failed to delete pulsar job details in Redis")
	}
	return nil
}

func (repo *RedisJobRepository) leaseJobs(clusterId string, jobIdsByQueue map[string][]string) (map[string][]string, error) {
	now := time.Now()
	pipe := repo.db.Pipeline()

	// Calling run on a script should automatically load the script into server-side cache.
	// However, calling script.Run() without first calling script.Load() results in a NOSCRIPT error,
	// perhaps due to a bug in go-redis.
	leaseJobScript.Load(pipe)
	cmds := make(map[string]*redis.Cmd)
	for queue, jobIds := range jobIdsByQueue {
		for _, jobId := range jobIds {
			cmds[jobId] = leaseJob(pipe, queue, clusterId, jobId, now)
		}
	}
	_, err := pipe.Exec()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	leasedJobIdsByQueue := make(map[string][]string, len(jobIdsByQueue))
	for queue, jobIds := range jobIdsByQueue {
		for _, jobId := range jobIds {
			cmd, ok := cmds[jobId]
			if !ok {
				continue
			}
			value, err := cmd.Int()
			if err != nil {
				log.Error(err)
			} else if value == alreadyAllocatedByDifferentCluster {
				log.WithField("jobId", jobId).Info("job already allocated to different cluster")
			} else if value == jobCancelled {
				log.WithField("jobId", jobId).Info("trying to renew cancelled job")
			} else {
				leasedJobIdsByQueue[queue] = append(leasedJobIdsByQueue[queue], jobId)
			}
		}
	}
	return leasedJobIdsByQueue, nil
}

func addJob(db redis.Cmdable, job *api.Job, jobData *[]byte) *redis.Cmd {
	return addJobScript.Run(db,
		[]string{
			jobQueuePrefix + job.Queue,
			jobObjectPrefix + job.Id,
			jobSetPrefix + job.JobSetId,
			jobSetPrefix + job.Queue + keySeparator + job.JobSetId,
			jobClientIdPrefix + job.Queue + keySeparator + job.ClientId,
			jobExistsPrefix + job.Id,
		},
		job.Id, job.Priority, *jobData, job.ClientId)
}

// This script will create the queue if it doesn't already exist.
// To avoid creating queues implicitly, code executing this script must ensure that the queue already exists.
var addJobScript = redis.NewScript(`
local queueKey = KEYS[1]
local jobKey = KEYS[2]
local jobSetKey = KEYS[3]
local jobSetQueueKey = KEYS[4]
local jobClientIdKey = KEYS[5]
local jobExistsKey = KEYS[6]

local jobId = ARGV[1]
local jobPriority = ARGV[2]
local jobData = ARGV[3]
local clientId = ARGV[4]


local jobExists = redis.call('EXISTS', jobExistsKey)
if jobExists == 1 then
	return '-1'
end

if clientId ~= '' then
	local existingJobId = redis.call('GET', jobClientIdKey)
	if existingJobId then
		return existingJobId
	end
	redis.call('SET', jobClientIdKey, jobId, 'EX', 14400)
end

redis.call('SET', jobExistsKey, '1', 'EX', 604800)
redis.call('SET', jobKey, jobData)
redis.call('SADD', jobSetKey, jobId)
redis.call('SADD', jobSetQueueKey, jobId)
redis.call('ZADD', queueKey, jobPriority, jobId)

return jobId
`)

func leaseJob(db redis.Cmdable, queueName string, clusterId string, jobId string, now time.Time) *redis.Cmd {
	return leaseJobScript.Run(db, []string{jobQueuePrefix + queueName, jobLeasedPrefix + queueName, jobClusterMapKey},
		clusterId, jobId, float64(now.UnixNano()))
}

const (
	alreadyAllocatedByDifferentCluster = -42
	jobCancelled                       = -43
)

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
