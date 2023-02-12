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
		running:   running,
		succeeded: succeeded,
		failed:    failed,
		cancelled: cancelled,
		returned:  returned,
	}
}

func (run *JobRun) Id() uuid.UUID {
	return run.id
}

func (run *JobRun) Executor() string {
	return run.executor
}

func (run *JobRun) Succeeded() bool {
	return run.succeeded
}

func (run *JobRun) WithSucceeded(succeeded bool) *JobRun {
	r := copyRun(*run)
	r.succeeded = succeeded
	return r
}

func (run *JobRun) Failed() bool {
	return run.failed
}

func (run *JobRun) WithFailed(failed bool) *JobRun {
	r := copyRun(*run)
	r.failed = failed
	return r
}

func (run *JobRun) Cancelled() bool {
	return run.cancelled
}

func (run *JobRun) WithCancelled(cancelled bool) *JobRun {
	r := copyRun(*run)
	r.cancelled = cancelled
	return r
}

func (run *JobRun) Returned() bool {
	return run.returned
}

func (run *JobRun) WithReturned(returned bool) *JobRun {
	r := copyRun(*run)
	r.returned = returned
	return r
}

func (run *JobRun) Created() int64 {
	return run.created
}

// InTerminalState returns true if the JobRun is in a terminal state
func (run *JobRun) InTerminalState() bool {
	return run.succeeded || run.failed || run.cancelled || run.returned
}

func copyRun(run JobRun) *JobRun {
	return &run
}
