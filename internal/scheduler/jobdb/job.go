package jobdb

import (
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulerJob is the scheduler-internal representation of a job.
type SchedulerJob struct {
	// String representation of the job id
	id string
	// Name of the queue this job belongs to.
	queue string
	// Jobset the job belongs to
	// We store this as it's needed for sending job event messages
	jobset string
	// Per-queue priority of this job.
	priority uint32
	// Logical timestamp indicating the order in which jobs are submitted.
	// Jobs with identical Queue and Priority
	// are sorted by timestamp.
	timestamp int64
	// True if the job is currently queued.
	// If this is set then the job will not be considered for scheduling
	queued bool
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	// True if the user has requested this job be cancelled
	cancelRequested bool
	// True if the scheduler has cancelled the job
	cancelled bool
	// True if the scheduler has failed the job
	failed bool
	// True if the scheduler has marked the job as succeeded
	succeeded bool
	// Job Runs by run id
	runsById map[uuid.UUID]*JobRun
	// The currently active run.  The run with the latest timestamp is the active run
	activeRun *JobRun
	// The timestamp of the currently active run.
	activeRunTimestamp int64
}

// NewJob creates a new scheduler job
func NewJob(
	jobId string,
	jobset string,
	queue string,
	priority uint32,
	schedulingInfo *schedulerobjects.JobSchedulingInfo,
	cancelRequested bool,
	cancelled bool,
	timestamp int64,
) *SchedulerJob {
	return &SchedulerJob{
		id:                jobId,
		jobset:            jobset,
		queue:             queue,
		queued:            true,
		priority:          priority,
		jobSchedulingInfo: schedulingInfo,
		cancelRequested:   cancelRequested,
		cancelled:         cancelled,
		timestamp:         timestamp,
		runsById:          map[uuid.UUID]*JobRun{},
	}
}

// GetId returns the id of the Job.
func (job *SchedulerJob) GetId() string {
	return job.id
}

// GetJobset returns the jobset  the job belongs to
func (job *SchedulerJob) GetJobset() string {
	return job.jobset
}

// GetQueue returns the queue this job belongs to.
func (job *SchedulerJob) GetQueue() string {
	return job.queue
}

// GetPriority returns the priority of the job
func (job *SchedulerJob) GetPriority() uint32 {
	return job.priority
}

// SetPriority sets the priority of the job
func (job *SchedulerJob) SetPriority(priority uint32) *SchedulerJob {
	copy := job.copy()
	copy.priority = priority
	return copy
}

func (job *SchedulerJob) GetJobSchedulingInfo() *schedulerobjects.JobSchedulingInfo {
	return job.jobSchedulingInfo
}

// GetRequirements is needed for compatibility with LegacySchedulerJob
func (job *SchedulerJob) GetRequirements(_ map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo {
	return job.jobSchedulingInfo
}

func (job *SchedulerJob) GetQueued() bool {
	return job.queued
}

func (job *SchedulerJob) SetQueued(queued bool) *SchedulerJob {
	copy := job.copy()
	copy.queued = queued
	return copy
}

func (job *SchedulerJob) GetCancelRequested() bool {
	return job.cancelRequested
}

func (job *SchedulerJob) SetCancelRequested() *SchedulerJob {
	copy := job.copy()
	copy.cancelRequested = true
	return copy
}

func (job *SchedulerJob) GetCancelled() bool {
	return job.cancelled
}

func (job *SchedulerJob) SetCancelled() *SchedulerJob {
	j := job.copy()
	j.cancelled = true
	return j
}

func (job *SchedulerJob) GetSucceeded() bool {
	return job.succeeded
}

func (job *SchedulerJob) SetSucceeded() *SchedulerJob {
	j := job.copy()
	j.cancelled = true
	return j
}

func (job *SchedulerJob) GetFailed() bool {
	return job.failed
}

func (job *SchedulerJob) SetFailed() *SchedulerJob {
	j := job.copy()
	j.failed = true
	return j
}

func (job *SchedulerJob) GetTimestamp() int64 {
	return job.timestamp
}

// GetAnnotations returns the annotations on the job.
func (job *SchedulerJob) GetAnnotations() map[string]string {
	requirements := job.jobSchedulingInfo.GetObjectRequirements()
	if len(requirements) == 0 {
		return nil
	}
	if podReqs := requirements[0].GetPodRequirements(); podReqs != nil {
		return podReqs.GetAnnotations()
	}
	return nil
}

// InTerminalState returns true if the job  is in a terminal state
func (job *SchedulerJob) InTerminalState() bool {
	return job.succeeded || job.cancelled || job.failed
}

func (job *SchedulerJob) HasCurrentRun() bool {
	return job.activeRun != nil
}

func (job *SchedulerJob) CreateRun(executor string) *SchedulerJob {
	j := job.copy()
	j.queued = false
	run := &JobRun{
		id:           uuid.New(),
		creationTime: time.Now().UnixNano(),
		executor:     executor,
	}
	if run.creationTime >= j.activeRunTimestamp {
		j.activeRunTimestamp = run.creationTime
		j.activeRun = run
	}
	j.runsById[run.id] = run
	return j
}

func (job *SchedulerJob) AddOrUpdateRun(run *JobRun) *SchedulerJob {
	j := job.copy()
	r := run.copy()
	if run.creationTime >= j.activeRunTimestamp {
		j.activeRunTimestamp = run.creationTime
		j.activeRun = r
	}
	j.runsById[run.id] = r
	return j
}

// NumReturned returns the number of times this job has been returned by executors
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small
func (job *SchedulerJob) NumReturned() uint {
	returned := uint(0)
	for _, run := range job.runsById {
		if run.returned {
			returned++
		}
	}
	return returned
}

// CurrentRun returns the currently active job run or nil if there are no runs yet
func (job *SchedulerJob) CurrentRun() *JobRun {
	return job.activeRun
}

// RunById returns the Run corresponding to the provided run id or nil if no such Run exists
func (job *SchedulerJob) RunById(id uuid.UUID) *JobRun {
	return job.runsById[id]
}

func (job *SchedulerJob) copy() *SchedulerJob {
	return &SchedulerJob{
		id:                 job.id,
		queue:              job.queue,
		jobset:             job.jobset,
		priority:           job.priority,
		timestamp:          job.timestamp,
		queued:             job.queued,
		jobSchedulingInfo:  job.jobSchedulingInfo,
		cancelRequested:    job.cancelRequested,
		cancelled:          job.cancelled,
		failed:             job.failed,
		succeeded:          job.succeeded,
		runsById:           maps.Clone(job.runsById),
		activeRun:          job.activeRun,
		activeRunTimestamp: job.activeRunTimestamp,
	}
}
