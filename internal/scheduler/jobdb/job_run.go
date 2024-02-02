package jobdb

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/scheduler/database"
)

// JobRun is the scheduler-internal representation of a job run.
//
// There are columns in the `runs` table that are not needed in the scheduler,
// such as `pod_requirements_overlay`; these are not represented here.
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
	// Priority class priority that this job was scheduled at.
	scheduledAtPriority *int32
	// True if the run has been reported as pending by the executor.
	pending bool
	// The time at which the run was reported as pending by the executor.
	pendingTime *time.Time
	// The time at which the run was leased to it's current executor.
	leaseTime *time.Time
	// True if the run has been reported as running by the executor.
	running bool
	// The time at which the run was reported as running by the executor.
	runningTime *time.Time
	// True if the run has been reported as preempted by the executor.
	preempted bool
	// The time at which the run was reported as preempted by the executor.
	preemptedTime *time.Time
	// True if the job has been reported as succeeded by the executor.
	succeeded bool
	// True if the job has been reported as failed by the executor.
	failed bool
	// True if the job has been reported as cancelled by the executor.
	cancelled bool
	// The time at which the job was reported as cancelled, failed or succeeded by the executor.
	terminatedTime *time.Time
	// True if the job has been returned by the executor.
	returned bool
	// True if the job has been returned and the job was given a chance to run.
	runAttempted bool
}

func (run *JobRun) String() string {
	// Include field names in string representation by default.
	return fmt.Sprintf("%#v", run)
}

// Assert makes the assertions outlined below and returns
// - nil if the run is valid and
// - an error explaining why not otherwise.
//
// Assertions:
// Required fields must be set.
//
// The states {Running, Cancelled, Failed, Succeeded, Returned} are mutually exclusive.
func (run *JobRun) Assert() error {
	if run == nil {
		return errors.Errorf("run is nil")
	}
	var result *multierror.Error

	// Assert that required fields are set.
	var emptyUuid uuid.UUID
	if run.Id() == emptyUuid {
		result = multierror.Append(result, errors.New("run has an empty id"))
	}
	if run.JobId() == "" {
		result = multierror.Append(result, errors.New("run has an empty jobId"))
	}
	if run.Executor() == "" {
		result = multierror.Append(result, errors.New("run has an empty executor"))
	}
	if run.NodeId() == "" {
		result = multierror.Append(result, errors.New("run has an empty nodeId"))
	}
	if run.NodeName() == "" {
		result = multierror.Append(result, errors.New("run has an empty nodeName"))
	}

	// Assertions specific to the state of the run.
	if run.Running() {
		if run.Succeeded() {
			result = multierror.Append(result, errors.New("run is marked as both running and succeeded"))
		}
		if run.Failed() {
			result = multierror.Append(result, errors.New("run is marked as both running and failed"))
		}
		if run.Cancelled() {
			result = multierror.Append(result, errors.New("run is marked as both running and cancelled"))
		}
		if run.Returned() {
			result = multierror.Append(result, errors.New("run is marked as both running and returned"))
		}
	} else if run.Succeeded() {
		if run.Failed() {
			result = multierror.Append(result, errors.New("run is marked as both succeeded and failed"))
		}
		if run.Cancelled() {
			result = multierror.Append(result, errors.New("run is marked as both succeeded and cancelled"))
		}
		if run.Returned() {
			result = multierror.Append(result, errors.New("run is marked as both succeeded and returned"))
		}
	} else if run.Failed() {
		if run.Cancelled() {
			result = multierror.Append(result, errors.New("run is marked as both failed and cancelled"))
		}
	} else if run.Cancelled() {
		if run.Returned() {
			result = multierror.Append(result, errors.New("run is marked as both cancelled and returned"))
		}
	} else if run.Returned() {
		// Nothing to do.
	} else {
		// Run is leased or pending; nothing to do.
	}
	if err := result.ErrorOrNil(); err != nil {
		// Avoid allocating the message "... invalid state" string if there were no errors.
		return errors.WithMessagef(err, "invalid run: %s", run)
	}
	return nil
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
	if run.id != other.id {
		return false
	}
	if run.jobId != other.jobId {
		return false
	}
	if run.created != other.created {
		return false
	}
	if run.executor != other.executor {
		return false
	}
	if run.nodeId != other.nodeId {
		return false
	}
	if run.nodeName != other.nodeName {
		return false
	}
	if run.scheduledAtPriority != other.scheduledAtPriority {
		if run.scheduledAtPriority != nil && other.scheduledAtPriority != nil {
			if *run.scheduledAtPriority != *other.scheduledAtPriority {
				return false
			}
		} else {
			return false
		}
	}
	if run.running != other.running {
		return false
	}
	if run.succeeded != other.succeeded {
		return false
	}
	if run.failed != other.failed {
		return false
	}
	if run.cancelled != other.cancelled {
		return false
	}
	if run.returned != other.returned {
		return false
	}
	if run.runAttempted != other.runAttempted {
		return false
	}
	return true
}

func MinimalRun(id uuid.UUID, creationTime int64) *JobRun {
	return &JobRun{
		id:                  id,
		created:             creationTime,
		scheduledAtPriority: nil,
	}
}

// CreateRun creates a new scheduler job run from a database job run
func (jobDb *JobDb) CreateRun(nodeId string, dbRun *database.Run) *JobRun {
	return &JobRun{
		id:                  dbRun.RunID,
		jobId:               dbRun.JobID,
		created:             dbRun.Created,
		executor:            jobDb.stringInterner.Intern(dbRun.Executor),
		nodeId:              jobDb.stringInterner.Intern(nodeId),
		nodeName:            jobDb.stringInterner.Intern(dbRun.Node),
		scheduledAtPriority: dbRun.ScheduledAtPriority,
		leaseTime:           dbRun.LeasedTimestamp,
		pendingTime:         dbRun.PendingTimestamp,
		running:             dbRun.Running,
		runningTime:         dbRun.RunningTimestamp,
		preempted:           dbRun.Preempted,
		preemptedTime:       dbRun.PreemptedTimestamp,
		succeeded:           dbRun.Succeeded,
		failed:              dbRun.Failed,
		cancelled:           dbRun.Cancelled,
		terminatedTime:      dbRun.TerminatedTimestamp,
		returned:            dbRun.Returned,
		runAttempted:        dbRun.RunAttempted,
	}
}

// Id returns the id of the JobRun.
func (run *JobRun) Id() uuid.UUID {
	return run.id
}

// JobId returns the id of the job this run is associated with.
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

// NodeName returns the name of the node to which the JobRun is assigned.
func (run *JobRun) NodeName() string {
	return run.nodeName
}

func (run *JobRun) ScheduledAtPriority() *int32 {
	return run.scheduledAtPriority
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

func (run *JobRun) Pending() bool {
	return run.pending
}

func (run *JobRun) WithPending(pending bool) *JobRun {
	run = run.DeepCopy()
	run.pending = pending
	return run
}

func (run *JobRun) PendingTime() *time.Time {
	return run.pendingTime
}

func (run *JobRun) LeaseTime() *time.Time {
	return run.leaseTime
}

// Running Returns true if the executor has reported the job run as running
func (run *JobRun) Running() bool {
	return run.running
}

func (run *JobRun) RunningTime() *time.Time {
	return run.runningTime
}

// WithRunning returns a copy of the job run with the running status updated.
func (run *JobRun) WithRunning(running bool) *JobRun {
	run = run.DeepCopy()
	run.running = running
	return run
}

// Preempted Returns true if the executor has reported the job run as preempted
func (run *JobRun) Preempted() bool {
	return run.preempted
}

func (run *JobRun) PreemptedTime() *time.Time {
	return run.preemptedTime
}

// WithRunning returns a copy of the job run with the running status updated.
func (run *JobRun) WithPreempted(preempted bool) *JobRun {
	run = run.DeepCopy()
	run.preempted = preempted
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

func (run *JobRun) TerminatedTime() *time.Time {
	return run.terminatedTime
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
