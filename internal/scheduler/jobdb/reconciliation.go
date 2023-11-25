package jobdb

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
)

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
	Deleted   bool
}

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
	Deleted   bool
}

func (jobDb *JobDb) ReconcileDifferences(txn *Txn, jobRepoJobs []database.Job, jobRepoRuns []database.Run) ([]JobStateTransitions, error) {
	jobRepoRunsById := armadaslices.MapAndGroupByFuncs(
		jobRepoRuns,
		func(jobRepoRun database.Run) string { return jobRepoRun.JobID },
		func(jobRepoRun database.Run) *database.Run { return &jobRepoRun },
	)
	jsts := make([]JobStateTransitions, len(jobRepoJobs))
	var err error
	for i, jobRepoJob := range jobRepoJobs {
		if jsts[i], err = jobDb.reconcileJobDifferences(
			txn.GetById(jobRepoJob.JobID),     // Existing job in the jobDb.
			&jobRepoJob,                       // New or updated job from the jobRepo.
			jobRepoRunsById[jobRepoJob.JobID], // New or updated runs associated with this job from the jobRepo.
		); err != nil {
			return nil, err
		}
	}
	return jsts, nil
}

// reconcileJobDifferences takes as its inputs for some job id
// - the job currently stored in the jobDb, or nil, if there is no such job,
// - the job stored in the job repository, or nil if there is no such job,
// - a slice composed of the runs associated with the job stored in the job repository,
// and returns a new jobdb.Job produced by reconciling any differences between the input jobs.
// It also returns a JobStateTransitions struct indicating which state transitions were applied.
// Multiple entries of JobStateTransitions may be true, e.g., if a job started running and failed since the last update.
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
		jst.Cancelled = jobRepoJob.Cancelled
		jst.Failed = jobRepoJob.Failed
		jst.Succeeded = jobRepoJob.Succeeded
	} else if job != nil && jobRepoJob == nil {
		job = nil
		jst.Deleted = true
		return
	} else if job != nil && jobRepoJob != nil {
		if jobRepoJob.CancelRequested && !job.CancelRequested() {
			job = job.WithCancelRequested(true)
		}
		if jobRepoJob.CancelByJobsetRequested && !job.CancelByJobsetRequested() {
			job = job.WithCancelByJobsetRequested(true)
		}
		if jobRepoJob.Cancelled && !job.Cancelled() {
			job = job.WithCancelled(true)
			jst.Cancelled = true
		}
		if jobRepoJob.Succeeded && !job.Succeeded() {
			job = job.WithSucceeded(true)
			jst.Succeeded = true
		}
		if jobRepoJob.Failed && !job.Failed() {
			job = job.WithFailed(true)
			jst.Failed = true
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
			jst.Queued = jobRepoJob.Queued
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

// TODO(albin): Preempted is not supported.
func (jobDb *JobDb) reconcileRunDifferences(jobRun *JobRun, jobRepoRun *database.Run) (rst RunStateTransitions) {
	defer func() { rst.JobRun = jobRun }()
	if jobRun == nil && jobRepoRun == nil {
		return
	} else if jobRun == nil && jobRepoRun != nil {
		jobRun = jobDb.schedulerRunFromDatabaseRun(jobRepoRun)
		rst.Leased = true
		rst.Pending = jobRepoRun.PendingTimestamp != nil
		rst.Running = jobRepoRun.Running
		rst.Cancelled = jobRepoRun.Cancelled
		rst.Failed = jobRepoRun.Failed
		rst.Succeeded = jobRepoRun.Succeeded
		return
	} else if jobRun != nil && jobRepoRun == nil {
		rst.Deleted = true
		return
	} else if jobRun != nil && jobRepoRun != nil {
		if jobRepoRun.Succeeded && !jobRun.Succeeded() {
			jobRun = jobRun.WithSucceeded(true)
			rst.Succeeded = true
		}
		if jobRepoRun.Failed && !jobRun.Failed() {
			jobRun = jobRun.WithFailed(true)
			rst.Failed = true
		}
		if jobRepoRun.Cancelled && !jobRun.Cancelled() {
			jobRun = jobRun.WithCancelled(true)
			rst.Cancelled = true
		}
		if jobRepoRun.Returned && !jobRun.Returned() {
			jobRun = jobRun.WithReturned(true)
			rst.Returned = true
		}
		if jobRepoRun.RunAttempted && !jobRun.RunAttempted() {
			jobRun = jobRun.WithAttempted(true)
		}
		return
	}
	return
}

// schedulerJobFromDatabaseJob creates a new scheduler job from a database job.
func (jobDb *JobDb) schedulerJobFromDatabaseJob(dbJob *database.Job) (*Job, error) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
	if err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo); err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling scheduling info for job %s", dbJob.JobID)
	}
	jobDb.internJobSchedulingInfoStrings(schedulingInfo)
	return jobDb.NewJob(
		dbJob.JobID,
		jobDb.stringInterner.Intern(dbJob.JobSet),
		jobDb.stringInterner.Intern(dbJob.Queue),
		uint32(dbJob.Priority),
		schedulingInfo,
		dbJob.Queued,
		dbJob.QueuedVersion,
		dbJob.CancelRequested,
		dbJob.CancelByJobsetRequested,
		dbJob.Cancelled,
		dbJob.Submitted,
	), nil
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
		dbRun.Running,
		dbRun.Succeeded,
		dbRun.Failed,
		dbRun.Cancelled,
		dbRun.Returned,
		dbRun.RunAttempted,
	)
}
