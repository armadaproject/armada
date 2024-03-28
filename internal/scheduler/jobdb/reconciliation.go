package jobdb

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"

	armadamath "github.com/armadaproject/armada/internal/common/math"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

// JobStateTransitions captures the process of updating a job.
// It bundles the updated job with booleans indicating which state transitions were applied to produce it.
// These are cumulative in the sense that a job with transitions queued -> scheduled -> queued -> running -> failed
// will have the fields queued, scheduled, running, and failed set to true.
type JobStateTransitions struct {
	Job *Job

	Queued    bool
	Leased    bool
	Pending   bool
	Running   bool
	Cancelled bool
	Preempted bool
	Failed    bool
	Succeeded bool
}

// applyRunStateTransitions applies the state transitions of a run to that of the associated job.
func (jst JobStateTransitions) applyRunStateTransitions(rst RunStateTransitions) JobStateTransitions {
	jst.Queued = jst.Queued || rst.Returned
	jst.Leased = jst.Leased || rst.Leased
	jst.Pending = jst.Pending || rst.Pending
	jst.Running = jst.Running || rst.Running
	jst.Cancelled = jst.Cancelled || rst.Cancelled
	jst.Preempted = jst.Preempted || rst.Preempted
	jst.Failed = jst.Failed || rst.Failed
	jst.Succeeded = jst.Succeeded || rst.Succeeded
	return jst
}

// RunStateTransitions captures the process of updating a run.
// It works in the same way as JobStateTransitions does for jobs.
type RunStateTransitions struct {
	JobRun *JobRun

	Leased    bool
	Returned  bool
	Pending   bool
	Running   bool
	Cancelled bool
	Preempted bool
	Failed    bool
	Succeeded bool
}

// ReconcileDifferences reconciles any differences between jobs stored in the jobDb with those provided to this function
// and returns the updated jobs together with a summary of the state transitions applied to those jobs.
func (jobDb *JobDb) ReconcileDifferences(txn *Txn, jobRepoJobs []database.Job, jobRepoRuns []database.Run) ([]JobStateTransitions, error) {
	// Map jobs for which a run was updated to nil and jobs updated directly to the updated job.
	jobRepoJobsById := make(map[string]*database.Job, armadamath.Max(len(jobRepoJobs), len(jobRepoRuns)))
	for _, jobRepoRun := range jobRepoRuns {
		jobRepoJobsById[jobRepoRun.JobID] = nil
	}
	for _, jobRepoJob := range jobRepoJobs {
		jobRepoJob := jobRepoJob
		jobRepoJobsById[jobRepoJob.JobID] = &jobRepoJob
	}

	// Group updated runs by the id of the job they're associated with.
	jobRepoRunsById := armadaslices.MapAndGroupByFuncs(
		jobRepoRuns,
		func(jobRepoRun database.Run) string { return jobRepoRun.JobID },
		func(jobRepoRun database.Run) *database.Run { return &jobRepoRun },
	)

	jsts := make([]JobStateTransitions, 0, len(jobRepoJobsById))
	for jobId, jobRepoJob := range jobRepoJobsById {
		if jst, err := jobDb.reconcileJobDifferences(
			txn.GetById(jobId),     // Existing job in the jobDb.
			jobRepoJob,             // New or updated job from the jobRepo.
			jobRepoRunsById[jobId], // New or updated runs associated with this job from the jobRepo.
		); err != nil {
			return nil, err
		} else {
			jsts = append(jsts, jst)
		}
	}
	return jsts, nil
}

// reconcileJobDifferences takes as its arguments for some job id
// - the job currently stored in the jobDb, or nil, if there is no such job,
// - the job stored in the job repository, or nil if there is no such job,
// - a slice composed of the runs associated with the job stored in the job repository,
// and returns a new jobdb.Job produced by reconciling any differences between the input jobs
// along with a summary of the state transitions applied to the job.
//
// TODO(albin): Pending, running, and preempted are not supported yet.
func (jobDb *JobDb) reconcileJobDifferences(job *Job, jobRepoJob *database.Job, jobRepoRuns []*database.Run) (jst JobStateTransitions, err error) {
	defer func() { jst.Job = job }()
	if job == nil && jobRepoJob == nil {
		return
	} else if job == nil && jobRepoJob != nil {
		if job, err = jobDb.schedulerJobFromDatabaseJob(jobRepoJob); err != nil {
			return
		}
		jst.Queued = true
	} else if job != nil && jobRepoJob == nil {
		// No direct updates to the job; just process any updated runs below.
	} else if job != nil && jobRepoJob != nil {
		if jobRepoJob.CancelRequested && !job.CancelRequested() {
			job = job.WithCancelRequested(true)
		}
		if jobRepoJob.CancelByJobsetRequested && !job.CancelByJobsetRequested() {
			job = job.WithCancelByJobsetRequested(true)
		}
		if jobRepoJob.Cancelled && !job.Cancelled() {
			job = job.WithCancelled(true)
		}
		if jobRepoJob.Succeeded && !job.Succeeded() {
			job = job.WithSucceeded(true)
		}
		if jobRepoJob.Failed && !job.Failed() {
			job = job.WithFailed(true)
		}
		if uint32(jobRepoJob.Priority) != job.RequestedPriority() {
			job = job.WithRequestedPriority(uint32(jobRepoJob.Priority))
		}
		if uint32(jobRepoJob.SchedulingInfoVersion) > job.JobSchedulingInfo().Version {
			schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
			if err = proto.Unmarshal(jobRepoJob.SchedulingInfo, schedulingInfo); err != nil {
				err = errors.Wrapf(err, "error unmarshalling scheduling info for job %s", jobRepoJob.JobID)
				return
			}
			job = job.WithJobSchedulingInfo(schedulingInfo)
		}
		if jobRepoJob.QueuedVersion > job.QueuedVersion() {
			job = job.WithQueuedVersion(jobRepoJob.QueuedVersion)
			job = job.WithQueued(jobRepoJob.Queued)
		}
	}

	// Reconcile run state transitions.
	for _, jobRepoRun := range jobRepoRuns {
		rst := jobDb.reconcileRunDifferences(job.RunById(jobRepoRun.RunID), jobRepoRun)
		jst = jst.applyRunStateTransitions(rst)
		job = job.WithUpdatedRun(rst.JobRun)
	}

	return
}

func (jobDb *JobDb) reconcileRunDifferences(jobRun *JobRun, jobRepoRun *database.Run) (rst RunStateTransitions) {
	defer func() { rst.JobRun = jobRun }()
	if jobRun == nil && jobRepoRun == nil {
		return
	} else if jobRun == nil && jobRepoRun != nil {
		jobRun = jobDb.schedulerRunFromDatabaseRun(jobRepoRun)
		rst.Returned = jobRepoRun.Returned
		rst.Pending = jobRepoRun.Pending
		rst.Leased = jobRepoRun.LeasedTimestamp != nil
		rst.Running = jobRepoRun.Running
		rst.Preempted = jobRepoRun.Preempted
		rst.Cancelled = jobRepoRun.Cancelled
		rst.Failed = jobRepoRun.Failed
		rst.Succeeded = jobRepoRun.Succeeded
	} else if jobRun != nil && jobRepoRun == nil {
		return
	} else if jobRun != nil && jobRepoRun != nil {
		if jobRepoRun.LeasedTimestamp != nil && !jobRun.Leased() {
			jobRun = jobRun.WithLeased(true).WithLeasedTime(jobRepoRun.LeasedTimestamp)
			rst.Leased = true
		}
		if jobRepoRun.Pending && !jobRun.Pending() {
			jobRun = jobRun.WithPending(true).WithPendingTime(jobRepoRun.PendingTimestamp)
			rst.Pending = true
		}
		if jobRepoRun.Running && !jobRun.Running() {
			jobRun = jobRun.WithRunning(true).WithRunningTime(jobRepoRun.RunningTimestamp)
			rst.Running = true
		}
		if jobRepoRun.Preempted && !jobRun.Preempted() {
			jobRun = jobRun.WithPreempted(true).WithRunning(false).WithPreemptedTime(jobRepoRun.PreemptedTimestamp)
			rst.Preempted = true
		}
		if jobRepoRun.Cancelled && !jobRun.Cancelled() {
			jobRun = jobRun.WithCancelled(true).WithRunning(false).WithTerminatedTime(jobRepoRun.TerminatedTimestamp)
			rst.Cancelled = true
		}
		if jobRepoRun.Failed && !jobRun.Failed() {
			jobRun = jobRun.WithFailed(true).WithRunning(false).WithTerminatedTime(jobRepoRun.TerminatedTimestamp)
			rst.Failed = true
		}
		if jobRepoRun.Succeeded && !jobRun.Succeeded() {
			jobRun = jobRun.WithSucceeded(true).WithRunning(false).WithTerminatedTime(jobRepoRun.TerminatedTimestamp)
			rst.Succeeded = true
		}
		if jobRepoRun.Returned && !jobRun.Returned() {
			jobRun = jobRun.WithReturned(true).WithRunning(false)
			rst.Returned = true
		}
		if jobRepoRun.RunAttempted && !jobRun.RunAttempted() {
			jobRun = jobRun.WithAttempted(true)
		}
	}
	jobRun = jobDb.enforceTerminalStateExclusivity(jobRun, &rst)
	return
}

// enforceTerminalStateExclusivity ensures that a job run has a single terminal state regardless of what the database reports.
// terminal states are: preempted, cancelled, failed, and succeeded.
func (jobDb *JobDb) enforceTerminalStateExclusivity(jobRun *JobRun, rst *RunStateTransitions) *JobRun {
	if jobRun.Succeeded() {
		rst.Preempted, rst.Cancelled, rst.Failed, rst.Succeeded = false, false, false, true
		return jobRun.WithoutTerminal().WithSucceeded(true)
	}
	if jobRun.Failed() {
		rst.Preempted, rst.Cancelled, rst.Succeeded, rst.Failed = false, false, false, true
		return jobRun.WithoutTerminal().WithFailed(true)
	}
	if jobRun.Cancelled() {
		rst.Preempted, rst.Failed, rst.Succeeded, rst.Cancelled = false, false, false, true
		return jobRun.WithoutTerminal().WithCancelled(true)
	}
	if jobRun.Preempted() {
		rst.Cancelled, rst.Failed, rst.Succeeded, rst.Preempted = false, false, false, true
		return jobRun.WithoutTerminal().WithPreempted(true)
	}
	return jobRun
}

// schedulerJobFromDatabaseJob creates a new scheduler job from a database job.
func (jobDb *JobDb) schedulerJobFromDatabaseJob(dbJob *database.Job) (*Job, error) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
	if err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo); err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling scheduling info for job %s", dbJob.JobID)
	}

	// Modify the resource requirements so that limits and requests point to the same object. This saves memory because
	// we no longer have to store both objects, while it is safe because at the api we assert that limits and requests
	// must be equal. Long term this is undesirable as if we ever want to have limits != requests this trick will not work.
	// instead we should find a more efficient mechanism for representing this data
	if schedulingInfo.GetPodRequirements() != nil {
		resourceRequirements := schedulingInfo.GetPodRequirements().GetResourceRequirements()
		schedulingInfo.GetPodRequirements().ResourceRequirements = v1.ResourceRequirements{
			Limits:   resourceRequirements.Limits,
			Requests: resourceRequirements.Limits,
		}
	}

	job := jobDb.NewJob(
		dbJob.JobID,
		dbJob.JobSet,
		dbJob.Queue,
		uint32(dbJob.Priority),
		schedulingInfo,
		dbJob.Queued,
		dbJob.QueuedVersion,
		dbJob.CancelRequested,
		dbJob.CancelByJobsetRequested,
		dbJob.Cancelled,
		dbJob.Submitted,
	)
	if dbJob.Failed {
		// TODO(albin): Let's make this an argument to NewJob. Even better: have the state as an enum argument.
		job = job.WithFailed(dbJob.Failed)
	}
	if dbJob.Succeeded {
		// TODO(albin): Same comment as the above.
		job = job.WithSucceeded(dbJob.Succeeded)
	}
	if uint32(dbJob.Priority) != job.RequestedPriority() {
		// TODO(albin): Same comment as the above.
		job = job.WithRequestedPriority(uint32(dbJob.Priority))
	}
	return job, nil
}

// schedulerRunFromDatabaseRun creates a new scheduler job run from a database job run
func (jobDb *JobDb) schedulerRunFromDatabaseRun(dbRun *database.Run) *JobRun {
	nodeId := api.NodeIdFromExecutorAndNodeName(dbRun.Executor, dbRun.Node)
	return jobDb.CreateRun(
		dbRun.RunID,
		dbRun.JobID,
		dbRun.Created,
		dbRun.Executor,
		nodeId,
		dbRun.Node,
		dbRun.ScheduledAtPriority,
		dbRun.LeasedTimestamp != nil,
		dbRun.Pending,
		dbRun.Running,
		dbRun.Preempted,
		dbRun.Succeeded,
		dbRun.Failed,
		dbRun.Cancelled,
		dbRun.LeasedTimestamp,
		dbRun.PendingTimestamp,
		dbRun.RunningTimestamp,
		dbRun.PreemptedTimestamp,
		dbRun.TerminatedTimestamp,
		dbRun.Returned,
		dbRun.RunAttempted,
	)
}
