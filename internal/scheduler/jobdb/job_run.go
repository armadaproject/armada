package jobdb

import (
	"github.com/google/uuid"
)

// JobRun is the scheduler-internal representation of a job run.
type JobRun struct {
	// Unique identifier for the run.
	id uuid.UUID
	// Id of the job this run is associated with.
	jobId string
	// Time at which the run was created.
	created int64
	// The name of the executor this run has been leased to.
	executor string
	// The id of the node this run has been leased to.
	// Identifies the node within the Armada scheduler.
	nodeId string
	// The name of the node this run has been leased to.
	// Identifies the node within the target executor cluster.
	nodeName string
	// True if the job has been reported as running by the executor.
	running bool
	// True if the job has been reported as succeeded by the executor.
	succeeded bool
	// True if the job has been reported as failed by the executor.
	failed bool
	// True if the job has been reported as cancelled by the executor.
	cancelled bool
	// True if the job has been returned by the executor.
	returned bool
	// True if the job has been returned and the job was given a chance to run.
	runAttempted bool
}

func (run *JobRun) Equal(other *JobRun) bool {
	if run == other {
		return true
	}
	if run == nil && other != nil {
		return false
	}
	if run != nil && other == nil {
		return false
	}
	return *run == *other
}

func MinimalRun(id uuid.UUID, creationTime int64) *JobRun {
	return &JobRun{
		id:      id,
		created: creationTime,
	}
}

// CreateRun creates a new scheduler job run from a database job run
func CreateRun(
	id uuid.UUID,
	jobId string,
	creationTime int64,
	executor string,
	nodeId string,
	nodeName string,
	running bool,
	succeeded bool,
	failed bool,
	cancelled bool,
	returned bool,
	runAttempted bool,
) *JobRun {
	return &JobRun{
		id:           id,
		jobId:        jobId,
		created:      creationTime,
		executor:     executor,
		nodeId:       nodeId,
		nodeName:     nodeName,
		running:      running,
		succeeded:    succeeded,
		failed:       failed,
		cancelled:    cancelled,
		returned:     returned,
		runAttempted: runAttempted,
	}
}

// Id returns the id of the JobRun.
func (run *JobRun) Id() uuid.UUID {
	return run.id
}

// Id returns the id of the job this run is associated with.
func (run *JobRun) JobId() string {
	return run.jobId
}

// Executor returns the executor to which the JobRun is assigned.
func (run *JobRun) Executor() string {
	return run.executor
}

// NodeId returns the id of the node to which the JobRun is assigned.
func (run *JobRun) NodeId() string {
	return run.nodeId
}

// NodeId returns the name of the node to which the JobRun is assigned.
func (run *JobRun) NodeName() string {
	return run.nodeName
}

// Succeeded Returns true if the executor has reported the job run as successful
func (run *JobRun) Succeeded() bool {
	return run.succeeded
}

// WithSucceeded returns a copy of the job run with the succeeded status updated.
func (run *JobRun) WithSucceeded(succeeded bool) *JobRun {
	run = run.DeepCopy()
	run.succeeded = succeeded
	return run
}

// Failed Returns true if the executor has reported the job run as failed
func (run *JobRun) Failed() bool {
	return run.failed
}

// WithFailed returns a copy of the job run with the failed status updated.
func (run *JobRun) WithFailed(failed bool) *JobRun {
	run = run.DeepCopy()
	run.failed = failed
	return run
}

// Cancelled Returns true if the user has cancelled the job run
func (run *JobRun) Cancelled() bool {
	return run.cancelled
}

// WithCancelled returns a copy of the job run with the cancelled status updated.
func (run *JobRun) WithCancelled(cancelled bool) *JobRun {
	run = run.DeepCopy()
	run.cancelled = cancelled
	return run
}

// Running Returns true if the executor has reported the job run as running
func (run *JobRun) Running() bool {
	return run.running
}

// WithRunning returns a copy of the job run with the running status updated.
func (run *JobRun) WithRunning(running bool) *JobRun {
	run = run.DeepCopy()
	run.running = running
	return run
}

// Returned Returns true if the executor has returned the job run.
func (run *JobRun) Returned() bool {
	return run.returned
}

func (run *JobRun) WithReturned(returned bool) *JobRun {
	run = run.DeepCopy()
	run.returned = returned
	return run
}

// RunAttempted Returns true if the executor has attempted to run the job.
func (run *JobRun) RunAttempted() bool {
	return run.runAttempted
}

// WithAttempted returns a copy of the job run with the runAttempted status updated.
func (run *JobRun) WithAttempted(attempted bool) *JobRun {
	run = run.DeepCopy()
	run.runAttempted = attempted
	return run
}

// Created Returns the creation time of the job run
func (run *JobRun) Created() int64 {
	return run.created
}

// InTerminalState returns true if the JobRun is in a terminal state
func (run *JobRun) InTerminalState() bool {
	return run.succeeded || run.failed || run.cancelled || run.returned
}

func (run *JobRun) DeepCopy() *JobRun {
	v := *run
	return &v
}
