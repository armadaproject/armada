package jobdb

import (
	"github.com/google/uuid"
)

// JobRun is the scheduler-internal representation of a job run.
type JobRun struct {
	// Unique identifier for the run
	id uuid.UUID
	// The time the run was allocated
	creationTime int64
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
		id:           id,
		creationTime: creationTime,
		executor:     executor,
		running:      running,
		succeeded:    succeeded,
		failed:       failed,
		cancelled:    cancelled,
		returned:     returned,
	}
}

func (run *JobRun) GetId() uuid.UUID {
	return run.id
}

func (run *JobRun) GetExecutor() string {
	return run.executor
}

func (run *JobRun) GetSucceeded() bool {
	return run.succeeded
}

func (run *JobRun) SetSucceeded() *JobRun {
	r := run.copy()
	r.succeeded = true
	return r
}

func (run *JobRun) GetFailed() bool {
	return run.failed
}

func (run *JobRun) SetFailed() *JobRun {
	r := run.copy()
	r.failed = true
	return r
}

func (run *JobRun) UnsetFailed() *JobRun {
	r := run.copy()
	r.failed = false
	return r
}

func (run *JobRun) GetCancelled() bool {
	return run.cancelled
}

func (run *JobRun) SetCancelled() *JobRun {
	r := run.copy()
	r.cancelled = true
	return r
}

func (run *JobRun) GetReturned() bool {
	return run.returned
}

func (run *JobRun) SetReturned() *JobRun {
	r := run.copy()
	r.returned = true
	return r
}

func (run *JobRun) GetCreated() int64 {
	return run.creationTime
}

// InTerminalState returns true if the JobRun is in a terminal state
func (run *JobRun) InTerminalState() bool {
	return run.succeeded || run.failed || run.cancelled || run.returned
}

// DeepCopy deep copies the entire JobRun
// This is needed because when runs are stored in the JobDb they cannot be modified in-place
func (run *JobRun) copy() *JobRun {
	return &JobRun{
		id:           run.id,
		creationTime: run.creationTime,
		executor:     run.executor,
		running:      run.running,
		succeeded:    run.succeeded,
		failed:       run.failed,
		cancelled:    run.cancelled,
		returned:     run.returned,
	}
}
