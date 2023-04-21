package jobdb

import (
	"time"

	"github.com/google/uuid"
	"golang.org/x/exp/maps"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// Job is the scheduler-internal representation of a job.
type Job struct {
	// String representation of the job id
	id string
	// Name of the queue this job belongs to.
	queue string
	// Jobset the job belongs to
	// We store this as it's needed for sending job event messages
	jobset string
	// Per-queue priority of this job.
	priority uint32
	// Requested per queue priority of this job.
	// This is used when syncing the postgres database with the scheduler-internal database
	requestedPriority uint32
	// Logical timestamp indicating the order in which jobs are submitted.
	// Jobs with identical Queue and Priority are sorted by this.
	created int64
	// True if the job is currently queued.
	// If this is set then the job will not be considered for scheduling
	queued bool
	// The current version of the queued state
	queuedVersion int32
	// Scheduling requirements of this job.
	jobSchedulingInfo *schedulerobjects.JobSchedulingInfo
	// True if the user has requested this job be cancelled
	cancelRequested bool
	// True if the user has requested this job's jobset be cancelled
	cancelByJobsetRequested bool
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

func EmptyJob(id string) *Job {
	return &Job{id: id, runsById: map[uuid.UUID]*JobRun{}}
}

// NewJob creates a new scheduler job
func NewJob(
	jobId string,
	jobset string,
	queue string,
	priority uint32,
	schedulingInfo *schedulerobjects.JobSchedulingInfo,
	queued bool,
	queuedVersion int32,
	cancelRequested bool,
	cancelByJobsetRequested bool,
	cancelled bool,
	created int64,
) *Job {
	return &Job{
		id:                      jobId,
		jobset:                  jobset,
		queue:                   queue,
		queued:                  queued,
		queuedVersion:           queuedVersion,
		priority:                priority,
		requestedPriority:       priority,
		jobSchedulingInfo:       schedulingInfo,
		cancelRequested:         cancelRequested,
		cancelByJobsetRequested: cancelByJobsetRequested,
		cancelled:               cancelled,
		created:                 created,
		runsById:                map[uuid.UUID]*JobRun{},
	}
}

// Id returns the id of the Job.
func (job *Job) Id() string {
	return job.id
}

// GetId returns the id of the Job.
// This is needed for the LegacyJob interface.
func (job *Job) GetId() string {
	return job.id
}

// Jobset returns the jobset the job belongs to.
func (job *Job) Jobset() string {
	return job.jobset
}

// GetJobSet returns the jobset the job belongs to.
// This is needed for compatibility with legacyJob
func (job *Job) GetJobSet() string {
	return job.jobset
}

// Queue returns the queue this job belongs to.
func (job *Job) Queue() string {
	return job.queue
}

// GetQueue returns the queue this job belongs to.
// This is needed for the LegacyJob interface.
func (job *Job) GetQueue() string {
	return job.queue
}

// Priority returns the priority of the job.
func (job *Job) Priority() uint32 {
	return job.priority
}

// RequestedPriority returns the requested priority of the job.
func (job *Job) RequestedPriority() uint32 {
	return job.requestedPriority
}

// WithPriority returns a copy of the job with the priority updated.
func (job *Job) WithPriority(priority uint32) *Job {
	j := copyJob(*job)
	j.priority = priority
	return j
}

// WithRequestedPriority returns a copy of the job with the priority updated.
func (job *Job) WithRequestedPriority(priority uint32) *Job {
	j := copyJob(*job)
	j.requestedPriority = priority
	return j
}

// JobSchedulingInfo returns the scheduling requirements associated with the job
func (job *Job) JobSchedulingInfo() *schedulerobjects.JobSchedulingInfo {
	return job.jobSchedulingInfo
}

// GetRequirements  returns the scheduling requirements associated with the job.
// this is needed for compatibility with LegacySchedulerJob
func (job *Job) GetRequirements(_ map[string]configuration.PriorityClass) *schedulerobjects.JobSchedulingInfo {
	return job.JobSchedulingInfo()
}

// Queued returns true if the job should be considered by the scheduler for assignment or false otherwise.
func (job *Job) Queued() bool {
	return job.queued
}

// WithQueued returns a copy of the job with the queued status updated.
func (job *Job) WithQueued(queued bool) *Job {
	j := copyJob(*job)
	j.queued = queued
	return j
}

// QueuedVersion returns current queued state version.
func (job *Job) QueuedVersion() int32 {
	return job.queuedVersion
}

// WithQueuedVersion returns a copy of the job with the queued version updated.
func (job *Job) WithQueuedVersion(version int32) *Job {
	j := copyJob(*job)
	j.queuedVersion = version
	return j
}

// CancelRequested returns true if the user has requested this job be cancelled.
func (job *Job) CancelRequested() bool {
	return job.cancelRequested
}

// CancelByJobsetRequested returns true if the user has requested this job's jobset be cancelled.
func (job *Job) CancelByJobsetRequested() bool {
	return job.cancelByJobsetRequested
}

// WithCancelRequested returns a copy of the job with the cancelRequested status updated.
func (job *Job) WithCancelRequested(cancelRequested bool) *Job {
	j := copyJob(*job)
	j.cancelRequested = cancelRequested
	return j
}

// WithCancelByJobsetRequested returns a copy of the job with the cancelByJobsetRequested status updated.
func (job *Job) WithCancelByJobsetRequested(cancelByJobsetRequested bool) *Job {
	j := copyJob(*job)
	j.cancelByJobsetRequested = cancelByJobsetRequested
	return j
}

// Cancelled Returns true if the scheduler has cancelled the job
func (job *Job) Cancelled() bool {
	return job.cancelled
}

// WithCancelled returns a copy of the job with the cancelled status updated
func (job *Job) WithCancelled(cancelled bool) *Job {
	j := copyJob(*job)
	j.cancelled = cancelled
	return j
}

// Succeeded Returns true if the scheduler has marked the job as succeeded
func (job *Job) Succeeded() bool {
	return job.succeeded
}

// WithSucceeded returns a copy of the job with the succeeded status updated.
func (job *Job) WithSucceeded(succeeded bool) *Job {
	j := copyJob(*job)
	j.succeeded = succeeded
	return j
}

// Failed Returns true if the scheduler has marked the job as failed
func (job *Job) Failed() bool {
	return job.failed
}

// WithFailed returns a copy of the job with the failed status updated.
func (job *Job) WithFailed(failed bool) *Job {
	j := copyJob(*job)
	j.failed = failed
	return j
}

// Created Returns the creation time of the job
func (job *Job) Created() int64 {
	return job.created
}

// GetAnnotations returns the annotations on the job.
// This is needed for compatibility with LegacySchedulerJob
func (job *Job) GetAnnotations() map[string]string {
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
func (job *Job) InTerminalState() bool {
	return job.succeeded || job.cancelled || job.failed
}

// HasRuns returns true if the job has been run
// If this is returns true then LatestRun is guaranteed to return a non-nil value.
func (job *Job) HasRuns() bool {
	return job.activeRun != nil
}

// WithNewRun creates a copy of the job with a new run on the given executor.
func (job *Job) WithNewRun(executor string, node string) *Job {
	run := &JobRun{
		id:       uuid.New(),
		jobId:    job.id,
		created:  time.Now().UnixNano(),
		executor: executor,
		node:     node,
	}
	return job.WithUpdatedRun(run)
}

// WithUpdatedRun creates a copy of the job with run details updated.
func (job *Job) WithUpdatedRun(run *JobRun) *Job {
	j := copyJob(*job)
	j.runsById = maps.Clone(j.runsById)
	if run.created >= j.activeRunTimestamp {
		j.activeRunTimestamp = run.created
		j.activeRun = run
	}
	j.runsById[run.id] = run
	return j
}

// NumReturned returns the number of times this job has been returned by executors
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small.
func (job *Job) NumReturned() uint {
	returned := uint(0)
	for _, run := range job.runsById {
		if run.returned {
			returned++
		}
	}
	return returned
}

// NumAttempts returns the number of times the executors tried to run this job
// Note that this is O(N) on Runs, but this should be fine as the number of runs should be small.
func (job *Job) NumAttempts() uint {
	attempts := uint(0)
	for _, run := range job.runsById {
		if run.runAttempted {
			attempts++
		}
	}
	return attempts
}

// AllRuns returns all runs associated with job.
func (job *Job) AllRuns() []*JobRun {
	return maps.Values(job.runsById)
}

// LatestRun returns the currently active job run or nil if there are no runs yet.
// Callers should either guard against nil values explicitly or via HasRuns.
func (job *Job) LatestRun() *JobRun {
	return job.activeRun
}

// RunById returns the Run corresponding to the provided run id or nil if no such Run exists.
func (job *Job) RunById(id uuid.UUID) *JobRun {
	return job.runsById[id]
}

// WithJobset returns a copy of the job with the jobset updated.
func (job *Job) WithJobset(jobset string) *Job {
	j := copyJob(*job)
	j.jobset = jobset
	return j
}

// WithQueue returns a copy of the job with the queue updated.
func (job *Job) WithQueue(queue string) *Job {
	j := copyJob(*job)
	j.queue = queue
	return j
}

// WithCreated returns a copy of the job with the creation time updated.
func (job *Job) WithCreated(created int64) *Job {
	j := copyJob(*job)
	j.created = created
	return j
}

// WithJobSchedulingInfo returns a copy of the job with the job scheduling info updated.
func (job *Job) WithJobSchedulingInfo(jobSchedulingInfo *schedulerobjects.JobSchedulingInfo) *Job {
	j := copyJob(*job)
	j.jobSchedulingInfo = jobSchedulingInfo
	return j
}

// copyJob makes a copy of the job
func copyJob(j Job) *Job {
	return &j
}

type JobPriorityComparer struct{}

// Compare jobs first by priority then by created and finally by id.
// returns -1 if a should come before b, 1 if a should come after b and 0 if the two jobs are equal
func (j JobPriorityComparer) Compare(a, b *Job) int {
	if a == b {
		return 0
	}

	// Compare the jobs by priority
	if a.priority != b.priority {
		if a.priority > b.priority {
			return -1
		} else {
			return 1
		}
	}

	// If the jobs have the same priority, compare them by created timestamp
	if a.created != b.created {
		if a.created < b.created {
			return -1
		} else {
			return 1
		}
	}

	// If the jobs have the same priority and created timestamp, compare them by ID
	if a.id != b.id {
		if a.id < b.id {
			return -1
		} else {
			return 1
		}
	}

	// If the jobs have the same ID, return 0
	return 0
}
