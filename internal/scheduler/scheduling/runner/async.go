package runner

import (
	"context"
	"sync"

	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type RunState int

const (
	Idle         RunState = iota // no work pending or in flight
	RunRequested                 // Trigger has been called; goroutine has not yet picked it up
	Running                      // goroutine is inside schedulingAlgo.Schedule
	ResultReady                  // run completed; result awaiting GetSchedulerResult
	Stopped                      // ctx cancelled; goroutine has exited
)

// AsyncSchedulingRunner runs the algorithm on a background goroutine
//   - The goroutine schedules against an isolated dry-run jobDb txn
//   - The caller must call trigger to kick off scheduling, otherwise no result will ever be presented via GetSchedulerResult
//   - Trigger must be called after the caller commits its txn so the next run's dry-run txn includes the reconciled decisions
//   - The runner is not thread-safe and must be used with a single goroutine
type AsyncSchedulingRunner struct {
	schedulingAlgo scheduling.SchedulingAlgo
	jobDb          *jobdb.JobDb

	clock clock.Clock

	// wake nudges the goroutine to re-check state. Size-1 buffered so a
	// Trigger that fires in the gap before the goroutine parks on its
	// receive is not lost. State lives in mu; the channel carries no meaning.
	wake chan struct{}

	mu sync.Mutex
	// state is the lifecycle position of the runner.
	state RunState
	// result of the last completed background run, awaiting consumption.
	result *scheduleResult
	// cancelFn cancels the ctx of the in-flight Schedule call.
	// Non-nil only while state == running.
	cancelFn context.CancelFunc
	// cancelRequested is set by Reset to tell the goroutine to discard the
	// in-flight run's result rather than publish it.
	cancelRequested bool
	// runDone is closed by the goroutine when the in-flight run has returned
	// Non-nil only while state == running.
	runDone chan struct{}
}

type scheduleResult struct {
	schedulerResult *scheduling.SchedulerResult
	err             error
}

// NewAsyncSchedulingRunner returns a runner that schedules on a background goroutine bound to ctx.
// The goroutine exits when ctx is cancelled.
func NewAsyncSchedulingRunner(ctx *armadacontext.Context, schedulingAlgo scheduling.SchedulingAlgo, jobDb *jobdb.JobDb) *AsyncSchedulingRunner {
	r := &AsyncSchedulingRunner{
		schedulingAlgo: schedulingAlgo,
		jobDb:          jobDb,
		clock:          clock.RealClock{},
		wake:           make(chan struct{}, 1),
	}
	go r.run(ctx)
	return r
}

func (r *AsyncSchedulingRunner) Trigger() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != Idle {
		// A run is requested, in flight, or holding an unread result.
		// Drop this Trigger rather than overwrite pending work.
		return
	}

	r.state = RunRequested
	select {
	case r.wake <- struct{}{}:
	default:
		// wake already buffered; the goroutine will see state on its next read.
	}
}

func (r *AsyncSchedulingRunner) resetResult() {
	if r.state != ResultReady {
		return
	}
	r.state = Idle
	r.result = nil
}

func (r *AsyncSchedulingRunner) GetCurrentState() RunState {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

type resetDetails struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *AsyncSchedulingRunner) reset() *resetDetails {
	r.mu.Lock()
	defer r.mu.Unlock()

	var result *resetDetails
	switch r.state {
	case Idle, Stopped:
		// Nothing in flight and nothing pending.
	case RunRequested:
		// Requested but not yet picked up: un-request it. The goroutine
		// will observe idle and keep waiting.
		r.state = Idle
	case ResultReady:
		r.resetResult()
	case Running:
		cancel := r.cancelFn
		done := r.runDone
		r.cancelRequested = true
		// finishRun observes cancelRequested, drops the result, returns to
		// idle and closes done.
		result = &resetDetails{cancel, done}
	}
	return result
}

// Reset discards any pending result, cancels any in-flight Schedule call,
// and blocks until that call has returned.
// After Reset returns, the next Trigger starts a fresh run.
func (r *AsyncSchedulingRunner) Reset() {
	resetDetails := r.reset()
	if resetDetails != nil {
		resetDetails.cancel()
		<-resetDetails.done
	}
}

func (r *AsyncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != ResultReady {
		return nil, nil
	}

	res := r.result
	r.resetResult()

	if res.err != nil {
		return nil, res.err
	}

	result, err := r.reconcile(ctx, txn, res.schedulerResult)
	if err != nil {
		return nil, err
	}

	err = upsertSchedulerResult(txn, result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type runDetails struct {
	ctx    *armadacontext.Context
	cancel context.CancelFunc
	done   chan struct{}
}

func (r *AsyncSchedulingRunner) run(ctx *armadacontext.Context) {
	for {
		run := r.awaitRun(ctx)
		if run == nil {
			// ctx cancelled while waiting; goroutine exits.
			return
		}

		txn := r.jobDb.DryRunTxn()
		log.Info("async scheduling cycle started")
		result, err := r.schedulingAlgo.Schedule(run.ctx, txn)

		r.finishRun(result, err, run)
	}
}

func (r *AsyncSchedulingRunner) trySetupRun(ctx *armadacontext.Context) *runDetails {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != RunRequested {
		return nil
	}

	select {
	case <-ctx.Done():
		r.state = Stopped
		return nil
	default:
	}

	r.state = Running
	runCtx, cancel := armadacontext.WithCancel(ctx)
	done := make(chan struct{})
	r.cancelFn = cancel
	r.runDone = done
	return &runDetails{ctx: runCtx, cancel: cancel, done: done}
}

func (r *AsyncSchedulingRunner) setStopped() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = Stopped
}

// awaitRun blocks until a run should start or the ctx is cancelled
func (r *AsyncSchedulingRunner) awaitRun(ctx *armadacontext.Context) *runDetails {
	for {
		if run := r.trySetupRun(ctx); run != nil {
			return run
		}
		select {
		case <-r.wake:
		case <-ctx.Done():
			r.setStopped()
			return nil
		}
	}
}

// finishRun publishes the completed run's result (or drops it if Reset asked for cancellation)
func (r *AsyncSchedulingRunner) finishRun(result *scheduling.SchedulerResult, err error, run *runDetails) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancelFn = nil
	r.runDone = nil

	if r.cancelRequested {
		r.cancelRequested = false
		r.state = Idle
		log.Info("async scheduling cycle cancelled")
	} else {
		r.result = &scheduleResult{schedulerResult: result, err: err}
		r.state = ResultReady
		if err != nil {
			log.Warnf("async scheduling cycle completed with err %s", err)
		} else {
			log.Infof("async scheduling cycle completed with success in %s", result.GetDuration())
		}
	}

	run.cancel()
	close(run.done)
}

func upsertSchedulerResult(txn *jobdb.Txn, result *scheduling.SchedulerResult) error {
	err := txn.Upsert(scheduling.ScheduledJobsFromSchedulerResult(result))
	if err != nil {
		return err
	}

	err = txn.Upsert(scheduling.PreemptedJobsFromSchedulerResult(result))
	if err != nil {
		return err
	}

	err = txn.Upsert(scheduling.JobsFromFailedReconciliationResults(result.GetCombinedReconciliationResult().FailedJobs))
	if err != nil {
		return err
	}

	err = txn.Upsert(scheduling.JobsFromFailedReconciliationResults(result.GetCombinedReconciliationResult().PreemptedJobs))
	if err != nil {
		return err
	}

	return nil
}

func (r *AsyncSchedulingRunner) IsAsync() bool { return true }

// reconcile validates an async scheduling result against the current txn,
// dropping decisions that are no longer valid (e.g. job no longer queued, preempted job was cancelled etc)
func (r *AsyncSchedulingRunner) reconcile(ctx *armadacontext.Context, txn *jobdb.Txn, result *scheduling.SchedulerResult) (*scheduling.SchedulerResult, error) {
	for _, poolResult := range result.PoolResults {
		if poolResult.SchedulingResult != nil {
			err := r.updateScheduledJobs(ctx, txn, poolResult)
			if err != nil {
				return nil, err
			}

			err = r.updatePreemptedJobs(ctx, txn, poolResult)
			if err != nil {
				return nil, err
			}

			// The demand will have changed if jobs were removed, recalculate shares
			poolResult.SchedulingResult.SchedulingContext.UpdateFairShares()
		}

		if poolResult.ReconciliationResult != nil {
			r.updateReconciliationResult(ctx, txn, poolResult)
		}
	}

	return result, nil
}

func (r *AsyncSchedulingRunner) updateScheduledJobs(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) error {
	scheduledJobs := make([]*schedulercontext.JobSchedulingContext, 0, len(poolResult.SchedulingResult.ScheduledJobs))

	gangJobs := slices.Filter(poolResult.SchedulingResult.ScheduledJobs, func(jctx *schedulercontext.JobSchedulingContext) bool { return jctx.Job.IsInGang() })
	nonGangJobs := slices.Filter(poolResult.SchedulingResult.ScheduledJobs, func(jctx *schedulercontext.JobSchedulingContext) bool { return !jctx.Job.IsInGang() })

	// Process non-gang jobs
	for _, scheduledJob := range nonGangJobs {
		currentJob := txn.GetById(scheduledJob.JobId)
		if isQueuedJobActionable(currentJob) {
			scheduledJobs = append(scheduledJobs, scheduledJob)
		} else {
			_, err := poolResult.SchedulingResult.SchedulingContext.RemoveJob(scheduledJob)
			if err != nil {
				return err
			}
		}
	}

	// Process gang jobs
	// We must unschedule the whole gang if any are not actionable
	type gangJobInfo struct {
		scheduledJctx *schedulercontext.JobSchedulingContext
		isActionable  bool
	}

	gangs := make(map[gangKey][]*gangJobInfo, len(gangJobs))
	for _, scheduledJob := range gangJobs {
		currentJob := txn.GetById(scheduledJob.JobId)
		key := gangKey{scheduledJob.Job.Queue(), scheduledJob.Job.GetGangInfo().Id()}
		if _, exists := gangs[key]; !exists {
			gangs[key] = make([]*gangJobInfo, 0, scheduledJob.Job.GetGangInfo().Cardinality())
		}
		gangs[key] = append(gangs[key], &gangJobInfo{scheduledJctx: scheduledJob, isActionable: isQueuedJobActionable(currentJob)})
	}

	for key, gang := range gangs {
		if len(gang) == 0 {
			continue
		}

		if len(gang) != gang[0].scheduledJctx.Job.GetGangInfo().Cardinality() {
			ctx.Logger().Errorf("partial scheduled found for gang %s/%s - expected %d scheduled jobs got %d",
				key.queue, key.gangId, gang[0].scheduledJctx.Job.GetGangInfo().Cardinality(), len(gang))
		}

		allActionable := slices.AllFunc(gang, func(jobInfo *gangJobInfo) bool { return jobInfo.isActionable })
		if allActionable {
			for _, jobInfo := range gang {
				scheduledJobs = append(scheduledJobs, jobInfo.scheduledJctx)
			}
		} else {
			for _, jobInfo := range gang {
				if jobInfo.isActionable {
					// This job is still actionable so simply unschedule it rather than remove it entirely,
					//  as it still causes demand for the queue
					err := poolResult.SchedulingResult.SchedulingContext.UnscheduleJob(jobInfo.scheduledJctx)
					if err != nil {
						return err
					}
				} else {
					_, err := poolResult.SchedulingResult.SchedulingContext.RemoveJob(jobInfo.scheduledJctx)
					if err != nil {
						return err
					}
				}
			}
			poolResult.SchedulingResult.SchedulingContext.NumScheduledGangs--
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been scheduled on pool %s",
		len(poolResult.SchedulingResult.ScheduledJobs)-len(scheduledJobs), poolResult.Name)
	poolResult.SchedulingResult.ScheduledJobs = scheduledJobs
	return nil
}

func (r *AsyncSchedulingRunner) updatePreemptedJobs(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) error {
	preemptedJobs := make([]*schedulercontext.JobSchedulingContext, 0, len(poolResult.SchedulingResult.PreemptedJobs))
	for _, preemptedJob := range poolResult.SchedulingResult.PreemptedJobs {
		currentJob := txn.GetById(preemptedJob.JobId)
		if isJobActionable(currentJob) {
			preemptedJobs = append(preemptedJobs, preemptedJob)
		} else {
			_, err := poolResult.SchedulingResult.SchedulingContext.RemoveJob(preemptedJob)
			if err != nil {
				return err
			}
			delete(poolResult.SchedulingResult.AdditionalSchedulingInfo.EvictorResult.EvictedJctxsByJobId, preemptedJob.JobId)
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been preempted on pool %s",
		len(poolResult.SchedulingResult.PreemptedJobs)-len(preemptedJobs), poolResult.Name)
	poolResult.SchedulingResult.PreemptedJobs = preemptedJobs
	return nil
}

func (r *AsyncSchedulingRunner) updateReconciliationResult(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) {
	preemptedJobs := make([]*scheduling.FailedReconciliationResult, 0, len(poolResult.ReconciliationResult.PreemptedJobs))
	for _, preemptedJob := range poolResult.ReconciliationResult.PreemptedJobs {
		currentJob := txn.GetById(preemptedJob.Job.Id())
		if isJobActionable(currentJob) {
			preemptedJobs = append(preemptedJobs, preemptedJob)
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been preempted by reconciler on pool %s",
		len(poolResult.ReconciliationResult.PreemptedJobs)-len(preemptedJobs), poolResult.Name)
	poolResult.ReconciliationResult.PreemptedJobs = preemptedJobs

	failedJobs := make([]*scheduling.FailedReconciliationResult, 0, len(poolResult.ReconciliationResult.FailedJobs))
	for _, failedJob := range poolResult.ReconciliationResult.FailedJobs {
		currentJob := txn.GetById(failedJob.Job.Id())
		if isJobActionable(currentJob) {
			failedJobs = append(failedJobs, failedJob)
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been failed by reconciler on pool %s",
		len(poolResult.ReconciliationResult.FailedJobs)-len(failedJobs), poolResult.Name)
	poolResult.ReconciliationResult.FailedJobs = failedJobs
}

func isQueuedJobActionable(job *jobdb.Job) bool {
	return isJobActionable(job) && job.Queued()
}

func isJobActionable(job *jobdb.Job) bool {
	return job != nil && !job.InTerminalState()
}

type gangKey struct {
	queue  string
	gangId string
}
