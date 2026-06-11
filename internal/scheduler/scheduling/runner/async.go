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

type runState int

const (
	idle         runState = iota // no work pending or in flight
	runRequested                 // Trigger has been called; goroutine has not yet picked it up
	running                      // goroutine is inside schedulingAlgo.Schedule
	resultReady                  // goroutine is inside schedulingAlgo.Schedule
	disabled                     // goroutine is inside schedulingAlgo.Schedule
)

// asyncSchedulingRunner runs the algorithm on a background goroutine
//   - The goroutine schedules against an isolated dry-run jobDb txn
//   - The caller must call trigger to kick off scheduling, otherwise no result will ever be presented via GetSchedulerResult
//   - Trigger must be called after the caller commits its txn so the next run's dry-run txn includes the reconciled decisions
//   - The runner is not thread-safe and must be used with a single goroutine
type asyncSchedulingRunner struct {
	schedulingAlgo scheduling.SchedulingAlgo
	jobDb          *jobdb.JobDb

	clock clock.Clock

	// Used to wake main go routine to check for work
	wake chan struct{}

	mu sync.Mutex
	// state is the lifecycle position of the runner.
	state runState
	// result of the last completed background run, awaiting consumption.
	result *scheduleResult
	// cancelFn cancels the ctx of the in-flight Schedule call. Non-nil
	// only while state == stateRunning.
	cancelFn context.CancelFunc

	cancelRequested bool
	// runDone is closed by the goroutine when the in-flight run has
	// returned (either normally or due to cancellation). Non-nil only
	// while state == stateRunning.
	runDone chan struct{}
}

type scheduleResult struct {
	schedulerResult *scheduling.SchedulerResult
	err             error
}

// NewAsyncSchedulingRunner returns a runner that schedules on a background
// goroutine bound to ctx. The goroutine exits when ctx is cancelled.
func NewAsyncSchedulingRunner(ctx *armadacontext.Context, schedulingAlgo scheduling.SchedulingAlgo, jobDb *jobdb.JobDb) SchedulingRunner {
	r := &asyncSchedulingRunner{
		schedulingAlgo: schedulingAlgo,
		jobDb:          jobDb,
		clock:          clock.RealClock{},
		wake:           make(chan struct{}),
	}
	go r.run(ctx)
	return r
}

func (r *asyncSchedulingRunner) Trigger() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != idle {
		return
	}

	r.state = runRequested
	select {
	case r.wake <- struct{}{}:
	default:
		// main routine isn't waiting on the call, so skip
	}
}

func (r *asyncSchedulingRunner) resetResult() {
	if r.state != resultReady {
		return
	}
	r.state = idle
	r.result = nil
}

// Reset discards any pending result, cancels any in-flight Schedule call,
// and blocks until that call has returned. After Reset returns, the next
// Trigger starts a fresh run.
func (r *asyncSchedulingRunner) Reset() {
	r.mu.Lock()
	if r.state == idle || r.state == disabled || r.cancelRequested {
		r.mu.Unlock()
		return
	}

	if r.state == resultReady {
		r.resetResult()
		r.mu.Unlock()
		return
	}

	cancel := r.cancelFn
	done := r.runDone
	r.cancelRequested = true
	r.mu.Unlock()

	if cancel != nil {
		cancel()
		<-done
	}
}

func (r *asyncSchedulingRunner) GetSchedulerResult(ctx *armadacontext.Context, txn *jobdb.Txn) (*scheduling.SchedulerResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state != resultReady {
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

func (r *asyncSchedulingRunner) run(ctx *armadacontext.Context) {
	for {
		// Wait for state == stateRequested. State changes only happen under
		// mu, so the goroutine is guaranteed to see Trigger / Reset
		// transitions atomically.
		r.mu.Lock()
		for r.state != runRequested {
			r.mu.Unlock()
			select {
			case <-r.wake:
			case <-ctx.Done():
				r.mu.Lock()
				r.state = disabled
				r.mu.Unlock()
				return
			}
			r.mu.Lock()
		}

		r.state = running
		runCtx, cancel := armadacontext.WithCancel(ctx)
		done := make(chan struct{})
		r.cancelFn = cancel
		r.runDone = done
		r.mu.Unlock()

		txn := r.jobDb.DryRunTxn()
		log.Info("async scheduling cycle started")
		result, err := r.schedulingAlgo.Schedule(runCtx, txn)

		r.mu.Lock()

		select {
		case <-ctx.Done():
			r.state = disabled
			r.mu.Unlock()
			return
		default:
			// context isn't cancelled
		}

		r.cancelFn = nil
		r.runDone = nil

		if !r.cancelRequested {
			r.result = &scheduleResult{schedulerResult: result, err: err}
			r.state = resultReady
			log.Info("async scheduling cycle completed")
		} else {
			r.cancelRequested = false
			r.state = idle
			log.Info("async scheduling cycle cancelled")
		}

		r.mu.Unlock()
		cancel()
		close(done)
	}
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

func (r *asyncSchedulingRunner) IsAsync() bool { return true }

// reconcile validates an async scheduling result against the current txn,
// dropping decisions that are no longer valid (e.g. job no longer queued, preempted job was cancelled etc)
func (r *asyncSchedulingRunner) reconcile(ctx *armadacontext.Context, txn *jobdb.Txn, result *scheduling.SchedulerResult) (*scheduling.SchedulerResult, error) {
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

func (r *asyncSchedulingRunner) updateScheduledJobs(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) error {
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
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been scheduled", len(poolResult.SchedulingResult.ScheduledJobs)-len(scheduledJobs))
	poolResult.SchedulingResult.ScheduledJobs = scheduledJobs
	return nil
}

func (r *asyncSchedulingRunner) updatePreemptedJobs(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) error {
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
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been preempted", len(poolResult.SchedulingResult.PreemptedJobs)-len(preemptedJobs))
	poolResult.SchedulingResult.PreemptedJobs = preemptedJobs
	return nil
}

func (r *asyncSchedulingRunner) updateReconciliationResult(ctx *armadacontext.Context, txn *jobdb.Txn, poolResult *scheduling.PoolSchedulingResult) {
	preemptedJobs := make([]*scheduling.FailedReconciliationResult, 0, len(poolResult.ReconciliationResult.PreemptedJobs))
	for _, preemptedJob := range poolResult.ReconciliationResult.PreemptedJobs {
		currentJob := txn.GetById(preemptedJob.Job.Id())
		if isJobActionable(currentJob) {
			preemptedJobs = append(preemptedJobs, preemptedJob)
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been preempted by reconciler",
		len(poolResult.ReconciliationResult.PreemptedJobs)-len(preemptedJobs))
	poolResult.ReconciliationResult.PreemptedJobs = preemptedJobs

	failedJobs := make([]*scheduling.FailedReconciliationResult, 0, len(poolResult.ReconciliationResult.FailedJobs))
	for _, failedJob := range poolResult.ReconciliationResult.FailedJobs {
		currentJob := txn.GetById(failedJob.Job.Id())
		if isJobActionable(currentJob) {
			failedJobs = append(failedJobs, failedJob)
		}
	}
	ctx.Infof("scheduler result reconciler removed %d jobs that would have been failed by reconciler",
		len(poolResult.ReconciliationResult.FailedJobs)-len(failedJobs))
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
