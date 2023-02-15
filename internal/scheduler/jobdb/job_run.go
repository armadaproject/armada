package jobdb

import (
	"github.com/google/uuid"
)

// JobRun is the scheduler-internal representation of a job run.
type JobRun struct {
	// Unique identifier for the run
	id uuid.UUID
	// The time the run was allocated
	created int64
	// The name of the executor this run has been leased to
	executor string
	// The name of the nodes this run has been leased to
	node string
	// True if the job has been reported as running by the executor
	running bool
	// True if the job has been reported as succeeded by the executor
	succeeded bool
	// True if the job has been reported as failed by the executor
	failed bool
	// True if the job has been reported as cancelled by the executor
	cancelled bool
	// True if the job has been returned by the executor
	returned bool
}

// CreateRun creates a new scheduler job run from a database job run
func CreateRun(
	id uuid.UUID,
	creationTime int64,
	executor string,
	node string,
	running bool,
	succeeded bool,
	failed bool,
	cancelled bool,
	returned bool,
) *JobRun {
	return &JobRun{
		id:        id,
		created:   creationTime,
		executor:  executor,
		node:      node,
		running:   running,
		succeeded: succeeded,
		failed:    failed,
		cancelled: cancelled,
		returned:  returned,
	}
}

// Id returns the id of the JobRun.
func (run *JobRun) Id() uuid.UUID {
	return run.id
}

// Executor returns the executor to which the JobRun is assigned.
func (run *JobRun) Executor() string {
	return run.executor
}

// Node returns the node to which the JobRun is assigned.
func (run *JobRun) Node() string {
	return run.node
}

// Succeeded Returns true if the executor has reported the job run as successful
func (run *JobRun) Succeeded() bool {
	return run.succeeded
}

// WithSucceeded returns a copy of the job run with the succeeded status updated.
func (run *JobRun) WithSucceeded(succeeded bool) *JobRun {
	r := copyRun(*run)
	r.succeeded = succeeded
	return r
}

// Failed Returns true if the executor has reported the job run as failed
func (run *JobRun) Failed() bool {
	return run.failed
}

// WithFailed returns a copy of the job run with the failed status updated.
func (run *JobRun) WithFailed(failed bool) *JobRun {
	r := copyRun(*run)
	r.failed = failed
	return r
}

// Cancelled Returns true if the user has cancelled the job run
func (run *JobRun) Cancelled() bool {
	return run.cancelled
}

// WithCancelled returns a copy of the job run with the cancelled status updated.
func (run *JobRun) WithCancelled(cancelled bool) *JobRun {
	r := copyRun(*run)
	r.cancelled = cancelled
	return r
}

// Running Returns true if the executor has reported the job run as running
func (run *JobRun) Running() bool {
	return run.running
}

// WithRunning returns a copy of the job run with the running status updated.
func (run *JobRun) WithRunning(running bool) *JobRun {
	r := copyRun(*run)
	r.running = running
	return r
}

// Returned Returns true if the executor has returned the job run.
func (run *JobRun) Returned() bool {
	return run.returned
}

// WithReturned returns a copy of the job run with the returned status updated.
func (run *JobRun) WithReturned(returned bool) *JobRun {
	r := copyRun(*run)
	r.returned = returned
	return r
}

// Created Returns the creation time of the job run
func (run *JobRun) Created() int64 {
	return run.created
}

// InTerminalState returns true if the JobRun is in a terminal state
func (run *JobRun) InTerminalState() bool {
	return run.succeeded || run.failed || run.cancelled || run.returned
}

// copyRun makes a copy of the job run
func copyRun(run JobRun) *JobRun {
	return &run
}
