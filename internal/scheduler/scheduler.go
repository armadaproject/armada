package scheduler

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/renstrom/shortuuid"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/affinity"
	"github.com/armadaproject/armada/internal/scheduler/leader"
	"github.com/armadaproject/armada/internal/scheduler/metrics"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Scheduler is the main Armada scheduler.
// It periodically performs the following cycle:
// 1. Update state from postgres (via the jobRepository).
// 2. Determine if leader and exit if not.
// 3. Generate any necessary eventSequences resulting from the state update.
// 4. Expire any jobs assigned to clusters that have timed out.
// 5. Schedule jobs.
// 6. Publish any Armada eventSequences resulting from the scheduling cycle.
type Scheduler struct {
	// Provides job updates from Postgres.
	jobRepository database.JobRepository
	// Used to determine whether a cluster is active.
	executorRepository database.ExecutorRepository
	// Responsible for assigning jobs to nodes.
	// TODO: Confusing name. Change.
	schedulingAlgo scheduling.SchedulingAlgo
	// Tells us if we are leader. Only the leader may schedule jobs.
	leaderController leader.LeaderController
	// This is used to check if jobs are still schedulable.
	// Useful when we are adding node anti-affinities.
	submitChecker SubmitScheduleChecker
	// Responsible for publishing messages to Pulsar. Only the leader publishes.
	publisher Publisher
	// Minimum duration between scheduler cycles.
	cyclePeriod time.Duration
	// Minimum duration between Schedule() calls - calls that actually schedule new jobs.
	schedulePeriod time.Duration
	// Maximum number of times a job can be attempted before being considered failed.
	maxAttemptedRuns uint
	// The label used when setting node anti affinities.
	nodeIdLabel string
	// If an executor fails to report in for this amount of time,
	// all jobs assigned to that executor are cancelled.
	executorTimeout time.Duration
	// The time the previous scheduling round ended
	previousSchedulingRoundEnd time.Time
	// Used for timing decisions (e.g., sleep).
	// Injected here so that we can mock it out for testing.
	clock clock.WithTicker
	// Stores active jobs (i.e. queued or running).
	jobDb *jobdb.JobDb
	// Highest offset we've read from Postgres on the jobs table.
	jobsSerial int64
	// Highest offset we've read from Postgres on the job runs table.
	runsSerial int64
	// Function that is called every time a cycle is completed. Useful for testing.
	onCycleCompleted func()
	// Prometheus metrics which report the state of the scheduler
	metrics *metrics.Metrics
	// If true, enable scheduler assertions.
	// In particular, assert that the jobDb is in a valid state at the end of each cycle.
	enableAssertions bool
}

func NewScheduler(
	jobDb *jobdb.JobDb,
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	schedulingAlgo scheduling.SchedulingAlgo,
	leaderController leader.LeaderController,
	publisher Publisher,
	submitChecker SubmitScheduleChecker,
	cyclePeriod time.Duration,
	schedulePeriod time.Duration,
	executorTimeout time.Duration,
	maxAttemptedRuns uint,
	nodeIdLabel string,
	metrics *metrics.Metrics,
) (*Scheduler, error) {
	return &Scheduler{
		jobRepository:              jobRepository,
		executorRepository:         executorRepository,
		schedulingAlgo:             schedulingAlgo,
		leaderController:           leaderController,
		publisher:                  publisher,
		submitChecker:              submitChecker,
		jobDb:                      jobDb,
		clock:                      clock.RealClock{},
		cyclePeriod:                cyclePeriod,
		schedulePeriod:             schedulePeriod,
		previousSchedulingRoundEnd: time.Time{},
		executorTimeout:            executorTimeout,
		maxAttemptedRuns:           maxAttemptedRuns,
		nodeIdLabel:                nodeIdLabel,
		jobsSerial:                 -1,
		runsSerial:                 -1,
		metrics:                    metrics,
	}, nil
}

func (s *Scheduler) EnableAssertions() {
	s.enableAssertions = true
}

// Run enters the scheduling loop, which will continue until ctx is cancelled.
func (s *Scheduler) Run(ctx *armadacontext.Context) error {
	ctx.Infof("starting scheduler with cycle time %s", s.cyclePeriod)
	defer ctx.Info("scheduler stopped")

	// JobDb initialisation.
	start := s.clock.Now()
	if err := s.initialise(ctx); err != nil {
		return err
	}
	ctx.Infof("JobDb initialised in %s", s.clock.Since(start))

	ticker := s.clock.NewTicker(s.cyclePeriod)
	prevLeaderToken := leader.InvalidLeaderToken()
	for {
		select {
		case <-ctx.Done():
			ctx.Infof("context cancelled; returning.")
			return ctx.Err()
		case <-ticker.C():
			start := s.clock.Now()
			ctx := armadacontext.WithLogField(ctx, "cycleId", shortuuid.New())
			leaderToken := s.leaderController.GetToken()
			fullUpdate := false
			ctx.Infof("received leaderToken; leader status is %t", leaderToken.Leader())

			// If we are becoming leader then we must ensure we have caught up to all Pulsar messages
			if leaderToken.Leader() && leaderToken != prevLeaderToken {
				ctx.Infof("becoming leader")
				syncContext, cancel := armadacontext.WithTimeout(ctx, 5*time.Minute)
				err := s.ensureDbUpToDate(syncContext, 1*time.Second)
				if err != nil {
					ctx.Logger().WithStacktrace(err).Error("could not become leader")
					leaderToken = leader.InvalidLeaderToken()
				} else {
					fullUpdate = true
				}
				cancel()
			}

			// Run a scheduler cycle.
			//
			// If there is an error, we can't guarantee that the scheduler-internal state is consistent with what was published
			// (scheduling decisions may have been partially published)
			// and we must invalidate the held leader token to trigger flushing Pulsar at the next cycle.
			//
			// TODO: Once the Pulsar client supports transactions, we can guarantee consistency even in case of errors.
			shouldSchedule := s.clock.Now().Sub(s.previousSchedulingRoundEnd) > s.schedulePeriod
			if !shouldSchedule {
				ctx.Info("Won't schedule this cycle as still within schedulePeriod")
			}

			result, err := s.cycle(ctx, fullUpdate, leaderToken, shouldSchedule)
			if err != nil {
				ctx.Logger().WithStacktrace(err).Error("scheduling cycle failure")
				leaderToken = leader.InvalidLeaderToken()
			}

			cycleTime := s.clock.Since(start)

			if shouldSchedule && leaderToken.Leader() {
				// Only the leader does real scheduling rounds.
				s.metrics.ReportScheduleCycleTime(cycleTime)
				s.metrics.ReportSchedulerResult(ctx, result)
				ctx.Infof("scheduling cycle completed in %s", cycleTime)
			} else {
				s.metrics.ReportReconcileCycleTime(cycleTime)
				ctx.Infof("reconciliation cycle completed in %s", cycleTime)
			}

			prevLeaderToken = leaderToken
			if s.onCycleCompleted != nil {
				s.onCycleCompleted()
			}
		}
	}
}

// cycle runs one iteration of the scheduling loop.
//
// Each iteration of the scheduler is functionally equivalent to the following:
//  1. Load the entire scheduler state, consisting of jobs, runs, and errors, from the persistent schedulerDb.
//  2. Generate job state transitions, which we can be seen as performing a mapping
//     (job_0, runs_0) -> (job_0', runs_0')
//     ...
//     (job_n, runs_n) -> (job_n', runs_n'),
//     where runs_i is the set of jobs associated with job_i, and there are n+1 jobs.
//     These mappings come in two categories:
//     - Triggered by an external event, e.g., jobs with a successful run should be marked as successful.
//     - Triggered by a scheduling decision, e.g., a scheduled job should have an additional run associated with it.
//  3. Generate eventSequences encoding these state transition and publish these to Pulsar.
//  4. Wait for these Pulsar eventSequences to be persisted to the schedulerDb.
//
// For performance reasons, the actual cycle differs from the above in three ways:
//   - In each cycle, we only load jobs, runs, and errors that have changed since the last cycle.
//     For unchanged jobs, runs, and errors, we rely on an in-memory cache maintained between cycles, i.e., the jobDb.
//   - Similarly, we only perform job state transitions for jobs with changed jobs and runs.
//     This is unless updateAll is true, in which case we perform state transitions for all jobs.
//     This is necessary for the first cycle after a leader failover.
//   - Instead of waiting for eventSequences to be persisted at the end of each cycle, the jobDb is updated directly.
//     This means we can start the next cycle immediately after one cycle finishes.
//     As state transitions are persisted and read back from the schedulerDb over later cycles,
//     there is no change to the jobDb, since the correct changes have already been made.
func (s *Scheduler) cycle(ctx *armadacontext.Context, updateAll bool, leaderToken leader.LeaderToken, shouldSchedule bool) (scheduling.SchedulerResult, error) {
	// TODO: Consider returning a slice of these instead.
	overallSchedulerResult := scheduling.SchedulerResult{}

	// Update job state.
	ctx.Info("Syncing internal state with database")
	updatedJobs, jsts, err := s.syncState(ctx, false)
	if err != nil {
		return overallSchedulerResult, err
	}
	ctx.Info("Finished syncing state")

	// Only the leader may make decisions; exit if not leader.
	// Only export metrics if leader.
	if !s.leaderController.ValidateToken(leaderToken) {
		ctx.Info("Not the leader so will not attempt to schedule")
		s.metrics.DisableLeaderMetrics()
		return overallSchedulerResult, err
	} else {
		s.metrics.EnableLeaderMetrics()
	}

	// If we've been asked to generate messages for all jobs, do so.
	// Otherwise, generate messages only for jobs updated this cycle.
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	if updateAll {
		updatedJobs = txn.GetAll()
	}

	// Load error messages for any failed runs.
	// TODO(albin): An unbounded number of job runs may fail between subsequent cycles.
	//              E.g., if 1M runs fail and each one generates a 12KiB error message, we'd need to load 12GiB of errors.
	//              (Each pod can produce at most 12KiB of errors; see
	//              https://kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/#customizing-the-termination-message)
	//              If so, the scheduler may not be able to progress until a human manually deleted those errors.
	failedRunIds := make([]string, 0, len(updatedJobs))
	for _, job := range updatedJobs {
		run := job.LatestRun()
		if run != nil && run.Failed() {
			failedRunIds = append(failedRunIds, run.Id())
		}
	}
	ctx.Info("Fetching job run errors")
	jobRepoRunErrorsByRunId, err := s.jobRepository.FetchJobRunErrors(ctx, failedRunIds)
	if err != nil {
		return overallSchedulerResult, err
	}
	ctx.Infof("Fetched %d job run errors", len(jobRepoRunErrorsByRunId))

	// Update metrics.
	if s.metrics.LeaderMetricsEnabled() {
		s.metrics.ReportStateTransitions(jsts, jobRepoRunErrorsByRunId)
	}

	// Generate any eventSequences that came out of synchronising the db state.
	ctx.Info("Generating update messages based on reconciliation changes")
	events, err := s.generateUpdateMessages(ctx, txn, updatedJobs, jobRepoRunErrorsByRunId)
	if err != nil {
		return overallSchedulerResult, err
	}
	ctx.Infof("Finished generating updates messages, generating %d events", len(events))

	// Validate that any new jobs can be scheduled
	validationEvents, err := s.submitCheck(ctx, txn)
	if err != nil {
		return overallSchedulerResult, err
	}
	events = append(events, validationEvents...)

	// Expire any jobs running on clusters that haven't heartbeated within the configured deadline.
	ctx.Info("Looking for jobs to expire")
	expirationEvents, err := s.expireJobsIfNecessary(ctx, txn)
	if err != nil {
		return overallSchedulerResult, err
	}
	ctx.Infof("Finished looking for jobs to expire, generating %d events", len(expirationEvents))
	events = append(events, expirationEvents...)

	// Schedule jobs.
	if shouldSchedule {
		var result *scheduling.SchedulerResult
		result, err = s.schedulingAlgo.Schedule(ctx, txn)
		if err != nil {
			return overallSchedulerResult, err
		}

		var resultEvents []*armadaevents.EventSequence
		resultEvents, err = s.eventsFromSchedulerResult(result)
		if err != nil {
			return overallSchedulerResult, err
		}
		events = append(events, resultEvents...)
		s.previousSchedulingRoundEnd = s.clock.Now()

		overallSchedulerResult = *result
	}

	// Publish to Pulsar.
	isLeader := func() bool {
		return s.leaderController.ValidateToken(leaderToken)
	}
	start := s.clock.Now()
	ctx.Infof("Starting to publish %d eventSequences to pulsar", len(events))
	if err = s.publisher.PublishMessages(ctx, events, isLeader); err != nil {
		return overallSchedulerResult, err
	}
	ctx.Infof("Published %d eventSequences to pulsar in %s", len(events), s.clock.Since(start))

	// Optionally assert that the jobDb is in a valid state and then commit.
	if s.enableAssertions {
		ctx.Infof("Performing assertions on current state")
		if err := txn.Assert(false); err != nil {
			return overallSchedulerResult, err
		}
	}
	ctx.Info("Committing cycle transaction")
	txn.Commit()
	ctx.Info("Completed committing cycle transaction")

	if s.metrics.LeaderMetricsEnabled() {
		for _, jctx := range overallSchedulerResult.ScheduledJobs {
			s.metrics.ReportJobLeased(jctx.Job)
		}
		for _, jctx := range overallSchedulerResult.PreemptedJobs {
			s.metrics.ReportJobPreempted(jctx.Job)
		}
	}

	return overallSchedulerResult, nil
}

// syncState updates jobs in jobDb to match state in postgres and returns all updated jobs.
func (s *Scheduler) syncState(ctx *armadacontext.Context, initial bool) ([]*jobdb.Job, []jobdb.JobStateTransitions, error) {
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	var updatedJobs []database.Job
	var updatedRuns []database.Run
	var err error

	if initial {
		// Load initial jobs from the jobRepo.
		initialJobs, initialRuns, maxJobSerial, maxRunSerial, fetchErr := s.jobRepository.FetchInitialJobs(ctx)
		if fetchErr != nil {
			return nil, nil, fmt.Errorf("fetching initial jobs: %w", fetchErr)
		}

		if len(initialJobs) > 0 {
			updatedJobs = initialJobs
		} else if maxJobSerial != nil {
			s.jobsSerial = *maxJobSerial // Allow the next sync to start from the highest serial possible.
		}

		if len(initialRuns) > 0 {
			updatedRuns = initialRuns
		} else if maxJobSerial != nil {
			s.runsSerial = *maxRunSerial // Allow the next sync to start from the highest serial possible.
		}

	} else {
		// Load new and updated jobs from the jobRepo.
		updatedJobs, updatedRuns, err = s.jobRepository.FetchJobUpdates(ctx, s.jobsSerial, s.runsSerial)
		if err != nil {
			return nil, nil, fmt.Errorf("fetching job updates: %w", err)
		}
	}

	// Reconcile any differences between the updated jobs and runs.
	jsts, err := s.jobDb.ReconcileDifferences(txn, updatedJobs, updatedRuns)
	if err != nil {
		return nil, nil, err
	}

	// Upsert updated jobs (including associated runs).
	jobDbJobs := make([]*jobdb.Job, 0, len(jsts))
	for _, jst := range jsts {
		jobDbJobs = append(jobDbJobs, jst.Job)
	}
	if err := txn.Upsert(jobDbJobs); err != nil {
		return nil, nil, err
	}

	// Delete jobs in a terminal state.
	idsOfJobsToDelete := make([]string, 0)
	for _, jobDbJob := range jobDbJobs {
		if jobDbJob.InTerminalState() {
			idsOfJobsToDelete = append(idsOfJobsToDelete, jobDbJob.Id())
		}
	}
	if err := txn.BatchDelete(idsOfJobsToDelete); err != nil {
		return nil, nil, err
	}

	txn.Commit()

	// Update serial to include these updates.
	if len(updatedJobs) > 0 {
		s.jobsSerial = updatedJobs[len(updatedJobs)-1].Serial
	}
	if len(updatedRuns) > 0 {
		s.runsSerial = updatedRuns[len(updatedRuns)-1].Serial
	}

	return jobDbJobs, jsts, nil
}

func (s *Scheduler) createSchedulingInfoWithNodeAntiAffinityForAttemptedRuns(job *jobdb.Job) (*internaltypes.JobSchedulingInfo, error) {
	newSchedulingInfo := job.JobSchedulingInfo().DeepCopy()
	newSchedulingInfo.Version = job.JobSchedulingInfo().Version + 1
	podRequirements := newSchedulingInfo.PodRequirements
	if podRequirements == nil {
		return nil, errors.Errorf("no pod scheduling requirement found for job %s", job.Id())
	}
	newAffinity := podRequirements.Affinity
	if newAffinity == nil {
		newAffinity = &v1.Affinity{}
	}

	for _, run := range job.AllRuns() {
		if run.RunAttempted() {
			err := affinity.AddNodeAntiAffinity(newAffinity, s.nodeIdLabel, run.NodeName())
			if err != nil {
				return nil, err
			}
		}
	}
	podRequirements.Affinity = newAffinity
	return newSchedulingInfo, nil
}

func (s *Scheduler) addNodeAntiAffinitiesForAttemptedRunsIfSchedulable(ctx *armadacontext.Context, job *jobdb.Job) (*jobdb.Job, bool, error) {
	schedulingInfoWithNodeAntiAffinity, err := s.createSchedulingInfoWithNodeAntiAffinityForAttemptedRuns(job)
	if err != nil {
		return nil, false, err
	}

	job, err = job.WithJobSchedulingInfo(schedulingInfoWithNodeAntiAffinity)
	if err != nil {
		// should never happen - requirements haven't changed
		panic(err)
	}
	results, err := s.submitChecker.Check(ctx, []*jobdb.Job{job})
	if err != nil {
		return nil, false, err
	}
	result, ok := results[job.Id()]
	isSchedulable := false
	if ok {
		isSchedulable = result.isSchedulable
	}
	return job, isSchedulable, nil
}

// eventsFromSchedulerResult generates necessary EventSequences from the provided SchedulerResult.
func (s *Scheduler) eventsFromSchedulerResult(result *scheduling.SchedulerResult) ([]*armadaevents.EventSequence, error) {
	return EventsFromSchedulerResult(result, s.clock.Now())
}

// EventsFromSchedulerResult generates necessary EventSequences from the provided SchedulerResult.
func EventsFromSchedulerResult(result *scheduling.SchedulerResult, time time.Time) ([]*armadaevents.EventSequence, error) {
	eventSequences := make([]*armadaevents.EventSequence, 0, len(result.PreemptedJobs)+len(result.ScheduledJobs))
	eventSequences, err := AppendEventSequencesFromPreemptedJobs(eventSequences, result.PreemptedJobs, time)
	if err != nil {
		return nil, err
	}
	eventSequences, err = AppendEventSequencesFromScheduledJobs(eventSequences, result.ScheduledJobs)
	if err != nil {
		return nil, err
	}
	return eventSequences, nil
}

func AppendEventSequencesFromPreemptedJobs(eventSequences []*armadaevents.EventSequence, jctxs []*schedulercontext.JobSchedulingContext, time time.Time) ([]*armadaevents.EventSequence, error) {
	for _, jctx := range jctxs {
		run := jctx.Job.LatestRun()
		if run == nil {
			return nil, errors.Errorf("attempting to generate preempted eventSequences for job %s with no associated runs", jctx.JobId)
		}
		eventSequences = append(eventSequences, &armadaevents.EventSequence{
			Queue:      jctx.Job.Queue(),
			JobSetName: jctx.Job.Jobset(),
			Events:     createEventsForPreemptedJob(jctx.Job, run.Id(), jctx.PreemptionDescription, time),
		})
	}
	return eventSequences, nil
}

func createEventsForPreemptedJob(job *jobdb.Job, runId string, reason string, time time.Time) []*armadaevents.EventSequence_Event {
	// Check for job failure in case of API preemption
	jobFailed := job.Failed()

	events := []*armadaevents.EventSequence_Event{
		{
			Created: protoutil.ToTimestamp(time),
			Event: &armadaevents.EventSequence_Event_JobRunPreempted{
				JobRunPreempted: &armadaevents.JobRunPreempted{
					PreemptedRunId: runId,
					PreemptedJobId: job.Id(),
					Reason:         reason,
				},
			},
		},
		{
			Created: protoutil.ToTimestamp(time),
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runId,
					JobId: job.Id(),
					Errors: []*armadaevents.Error{
						{
							Terminal: jobFailed,
							Reason: &armadaevents.Error_JobRunPreemptedError{
								JobRunPreemptedError: &armadaevents.JobRunPreemptedError{
									Reason: reason,
								},
							},
						},
					},
				},
			},
		},
	}

	if jobFailed {
		// Only send a job error event if the job is in a failed state
		events = append(events, &armadaevents.EventSequence_Event{
			Created: protoutil.ToTimestamp(time),
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: job.Id(),
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_JobRunPreemptedError{
								JobRunPreemptedError: &armadaevents.JobRunPreemptedError{
									Reason: reason,
								},
							},
						},
					},
				},
			},
		})
	}

	if job.Queued() {
		// Assume the job has already been marked as queued and the version has been bumped
		events = append(events, &armadaevents.EventSequence_Event{
			Created: protoutil.ToTimestamp(time),
			Event: &armadaevents.EventSequence_Event_JobRequeued{
				JobRequeued: &armadaevents.JobRequeued{
					JobId:                job.Id(),
					SchedulingInfo:       internaltypes.ToSchedulerObjectsJobSchedulingInfo(job.JobSchedulingInfo()),
					UpdateSequenceNumber: job.QueuedVersion(),
				},
			},
		})
	}

	return events
}

func AppendEventSequencesFromScheduledJobs(eventSequences []*armadaevents.EventSequence, jctxs []*schedulercontext.JobSchedulingContext) ([]*armadaevents.EventSequence, error) {
	for _, jctx := range jctxs {
		job := jctx.Job
		run := job.LatestRun()
		if run == nil {
			return nil, errors.Errorf("attempting to generate lease eventSequences for job %s with no associated runs", job.Id())
		}
		runCreationTime := time.Unix(0, job.ActiveRunTimestamp())
		scheduledAtPriority, hasScheduledAtPriority := job.ScheduledAtPriority()
		eventSequences = append(eventSequences, &armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(), // TODO: Rename to JobSet.
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: protoutil.ToTimestamp(runCreationTime),
					Event: &armadaevents.EventSequence_Event_JobRunLeased{
						JobRunLeased: &armadaevents.JobRunLeased{
							RunId:      run.Id(),
							JobId:      job.Id(),
							ExecutorId: run.Executor(),
							// NodeId here refers to the unique identifier of the node in an executor cluster,
							// which is referred to as the NodeName within the scheduler.
							NodeId:                 run.NodeName(),
							UpdateSequenceNumber:   job.QueuedVersion(),
							HasScheduledAtPriority: hasScheduledAtPriority,
							ScheduledAtPriority:    scheduledAtPriority,
							PodRequirementsOverlay: &schedulerobjects.PodRequirements{
								Tolerations: armadaslices.Map(jctx.AdditionalTolerations, func(t v1.Toleration) *v1.Toleration {
									return &t
								}),
							},
							Pool: run.Pool(),
						},
					},
				},
			},
		})
	}
	return eventSequences, nil
}

// generateUpdateMessages generates EventSequences representing the state changes on updated jobs.
// If there are no state changes then an empty slice will be returned.
func (s *Scheduler) generateUpdateMessages(ctx *armadacontext.Context, txn *jobdb.Txn, updatedJobs []*jobdb.Job, jobRunErrors map[string]*armadaevents.Error) ([]*armadaevents.EventSequence, error) {
	// Generate any eventSequences that came out of synchronising the db state.
	var events []*armadaevents.EventSequence
	for _, job := range updatedJobs {
		jobEvents, err := s.generateUpdateMessagesFromJob(ctx, job, jobRunErrors, txn)
		if err != nil {
			return nil, err
		}
		if jobEvents != nil {
			events = append(events, jobEvents)
		}
	}
	return events, nil
}

// generateUpdateMessages generates an EventSequence representing the state changes for a single job.
// If there are no state changes it returns nil.
func (s *Scheduler) generateUpdateMessagesFromJob(ctx *armadacontext.Context, job *jobdb.Job, jobRunErrors map[string]*armadaevents.Error, txn *jobdb.Txn) (*armadaevents.EventSequence, error) {
	var events []*armadaevents.EventSequence_Event

	// Is the job already in a terminal state? If so then don't send any more messages
	if job.InTerminalState() {
		return nil, nil
	}

	origJob := job

	if job.RequestedPriority() != job.Priority() {
		job = job.WithPriority(job.RequestedPriority())
		jobReprioritised := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
				ReprioritisedJob: &armadaevents.ReprioritisedJob{
					JobId:    job.Id(),
					Priority: job.Priority(),
				},
			},
		}
		events = append(events, jobReprioritised)
	}

	// Has the job been requested cancelled. If so, cancel the job
	if job.CancelRequested() {
		if job.HasRuns() {
			lastRun := job.LatestRun()
			job = job.WithUpdatedRun(lastRun.WithoutTerminal().WithCancelled(true))

			events = append(events, &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobRunCancelled{
					JobRunCancelled: &armadaevents.JobRunCancelled{
						RunId: lastRun.Id(),
						JobId: job.Id(),
					},
				},
			})
		}
		job = job.WithQueued(false).WithoutTerminal().WithCancelled(true)
		var cancelUser string
		if cancelUserPtr := job.CancelUser(); cancelUserPtr != nil {
			cancelUser = *cancelUserPtr
		}
		cancel := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId:      job.Id(),
					CancelUser: cancelUser,
				},
			},
		}
		events = append(events, cancel)
	} else if job.CancelByJobsetRequested() {
		job = job.WithQueued(false).WithoutTerminal().WithCancelled(true)
		var cancelUser string
		if cancelUserPtr := job.CancelUser(); cancelUserPtr != nil {
			cancelUser = *cancelUserPtr
		}
		cancelRequest := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: job.Id()},
			},
		}
		events = append(events, cancelRequest)

		if job.HasRuns() {
			lastRun := job.LatestRun()
			job = job.WithUpdatedRun(lastRun.WithoutTerminal().WithCancelled(true))

			events = append(events, &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobRunCancelled{
					JobRunCancelled: &armadaevents.JobRunCancelled{
						RunId: lastRun.Id(),
						JobId: job.Id(),
					},
				},
			})
		}

		cancel := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId:      job.Id(),
					CancelUser: cancelUser,
				},
			},
		}
		events = append(events, cancel)
	} else if job.HasRuns() {
		lastRun := job.LatestRun()
		// InTerminalState states. Can only have one of these
		if lastRun.Succeeded() {
			job = job.WithSucceeded(true).WithQueued(false)
			jobSucceeded := &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobSucceeded{
					JobSucceeded: &armadaevents.JobSucceeded{
						JobId: job.Id(),
					},
				},
			}
			events = append(events, jobSucceeded)
		} else if lastRun.Failed() && !job.Queued() {
			failFast := job.Annotations()[configuration.FailFastAnnotation] == "true"
			requeueJob := !failFast && lastRun.Returned() && job.NumAttempts() < s.maxAttemptedRuns

			if requeueJob && lastRun.RunAttempted() {
				jobWithAntiAffinity, schedulable, err := s.addNodeAntiAffinitiesForAttemptedRunsIfSchedulable(ctx, job)
				if err != nil {
					return nil, errors.Errorf("unable to set node anti-affinity for job %s because %s", job.Id(), err)
				} else {
					if schedulable {
						job = jobWithAntiAffinity
					} else {
						// If job is not schedulable with anti-affinity added. Do not requeue it and let it fail.
						requeueJob = false
					}
				}
			}

			if requeueJob {
				job = job.WithQueued(true)
				job = job.WithQueuedVersion(job.QueuedVersion() + 1)

				requeueJobEvent := &armadaevents.EventSequence_Event{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobRequeued{
						JobRequeued: &armadaevents.JobRequeued{
							JobId:                job.Id(),
							SchedulingInfo:       internaltypes.ToSchedulerObjectsJobSchedulingInfo(job.JobSchedulingInfo()),
							UpdateSequenceNumber: job.QueuedVersion(),
						},
					},
				}

				events = append(events, requeueJobEvent)
			} else {
				runError := jobRunErrors[lastRun.Id()]
				if runError == nil {
					return nil, errors.Errorf(
						"no run error found for run %s (job id = %s), this must mean we're out of sync with the database",
						lastRun.Id(), job.Id(),
					)
				}

				job = job.WithFailed(true).WithQueued(false)
				if lastRun.Returned() {
					errorMessage := fmt.Sprintf("Maximum number of attempts (%d) reached - this job will no longer be retried", s.maxAttemptedRuns)
					if job.NumAttempts() < s.maxAttemptedRuns {
						errorMessage = fmt.Sprintf("Job was attempted %d times, and has been tried once on all nodes it can run on - this job will no longer be retried", job.NumAttempts())
					}
					if failFast {
						errorMessage = fmt.Sprintf("Job has fail fast flag set - this job will no longer be retried")
					}

					if runError.GetPodLeaseReturned() != nil && runError.GetPodLeaseReturned().GetMessage() != "" {
						errorMessage += "\n\n" + "Final run error:"
						errorMessage += "\n" + runError.GetPodLeaseReturned().GetMessage()
					}

					runError = &armadaevents.Error{
						Terminal: true,
						Reason: &armadaevents.Error_MaxRunsExceeded{
							MaxRunsExceeded: &armadaevents.MaxRunsExceeded{
								Message: errorMessage,
							},
						},
					}
				}
				jobErrors := &armadaevents.EventSequence_Event{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobErrors{
						JobErrors: &armadaevents.JobErrors{
							JobId:  job.Id(),
							Errors: []*armadaevents.Error{runError},
						},
					},
				}

				events = append(events, jobErrors)
			}
		} else if lastRun.PreemptRequested() && job.PriorityClass().Preemptible {
			job = job.WithQueued(false).WithFailed(true).WithUpdatedRun(lastRun.WithoutTerminal().WithFailed(true))
			events = append(events, createEventsForPreemptedJob(job, lastRun.Id(), "Preempted - preemption requested via API", s.clock.Now())...)
		}
	}

	if !origJob.Equal(job) {
		if err := txn.Upsert([]*jobdb.Job{job}); err != nil {
			return nil, err
		}
	}

	if len(events) > 0 {
		return &armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events:     events,
		}, nil
	}

	return nil, nil
}

// expireJobsIfNecessary removes any jobs from the JobDb which are running on stale executors.
// It also generates an EventSequence for each job, indicating that both the run and the job has failed
// Note that this is different behaviour from the old scheduler which would allow expired jobs to be rerun
func (s *Scheduler) expireJobsIfNecessary(ctx *armadacontext.Context, txn *jobdb.Txn) ([]*armadaevents.EventSequence, error) {
	heartbeatTimes, err := s.executorRepository.GetLastUpdateTimes(ctx)
	if err != nil {
		return nil, err
	}
	staleExecutors := make(map[string]bool)
	cutOff := s.clock.Now().Add(-s.executorTimeout)

	jobsToUpdate := make([]*jobdb.Job, 0)

	// TODO: this will only detect stale clusters if they exist in the database
	// Right now this is fine because nothing will delete this jobs, but we should consider the case where an executor
	// has been completely removed
	for executor, heartbeat := range heartbeatTimes {
		if heartbeat.Before(cutOff) {
			ctx.Warnf("Executor %s has not reported a hearbeart since %v. Will expire all jobs running on this executor", executor, heartbeat)
			staleExecutors[executor] = true
		}
	}

	// All clusters have had a heartbeat recently.  No need to expire any jobs
	if len(staleExecutors) == 0 {
		ctx.Infof("No stale executors found. No jobs need to be expired")
		return nil, nil
	}

	events := make([]*armadaevents.EventSequence, 0)

	// TODO: this is inefficient. We should create a iterator of the jobs running on the affected executors
	jobs := txn.GetAll()

	for _, job := range jobs {

		if job.InTerminalState() {
			continue
		}

		run := job.LatestRun()
		if run != nil && !job.Queued() && staleExecutors[run.Executor()] {
			ctx.Warnf("Cancelling job %s as it is running on lost executor %s", job.Id(), run.Executor())
			jobsToUpdate = append(jobsToUpdate, job.WithQueued(false).WithFailed(true).WithUpdatedRun(run.WithFailed(true)))

			leaseExpiredError := &armadaevents.Error{
				Terminal: true,
				Reason: &armadaevents.Error_LeaseExpired{
					LeaseExpired: &armadaevents.LeaseExpired{},
				},
			}
			es := &armadaevents.EventSequence{
				Queue:      job.Queue(),
				JobSetName: job.Jobset(),
				Events: []*armadaevents.EventSequence_Event{
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobRunErrors{
							JobRunErrors: &armadaevents.JobRunErrors{
								RunId:  run.Id(),
								JobId:  job.Id(),
								Errors: []*armadaevents.Error{leaseExpiredError},
							},
						},
					},
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobErrors{
							JobErrors: &armadaevents.JobErrors{
								JobId:  job.Id(),
								Errors: []*armadaevents.Error{leaseExpiredError},
							},
						},
					},
				},
			}
			events = append(events, es)
		}
	}
	if err := txn.Upsert(jobsToUpdate); err != nil {
		return nil, err
	}
	return events, nil
}

func (s *Scheduler) submitCheck(ctx *armadacontext.Context, txn *jobdb.Txn) ([]*armadaevents.EventSequence, error) {
	jobsToCheck := make([]*jobdb.Job, 0)

	it := txn.UnvalidatedJobs()

	for job, _ := it.Next(); job != nil; job, _ = it.Next() {
		// Don't check jobs that are terminal
		if job.InTerminalState() {
			continue
		}
		jobsToCheck = append(jobsToCheck, job)
	}

	results, err := s.submitChecker.Check(ctx, jobsToCheck)
	if err != nil {
		return nil, err
	}

	for _, job := range jobsToCheck {
		err := job.ValidateResourceRequests()
		if err != nil {
			results[job.Id()] = schedulingResult{isSchedulable: false, reason: "invalid resource request: " + err.Error()}
		}
	}

	events := make([]*armadaevents.EventSequence, 0)
	jobsToUpdate := make([]*jobdb.Job, 0)
	for _, job := range jobsToCheck {
		result := results[job.Id()]

		es := &armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: s.now(),
				},
			},
		}

		if result.isSchedulable {
			job = job.WithValidated(true).WithPools(result.pools)
			jobsToUpdate = append(jobsToUpdate, job)

			es.Events[0].Event = &armadaevents.EventSequence_Event_JobValidated{
				JobValidated: &armadaevents.JobValidated{
					JobId: job.Id(),
					Pools: result.pools,
				},
			}
		} else {
			job = job.WithFailed(true).WithQueued(false)
			jobsToUpdate = append(jobsToUpdate, job)

			es.Events[0].Event = &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: job.Id(),
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason: &armadaevents.Error_JobRejected{
								JobRejected: &armadaevents.JobRejected{
									Message: result.reason,
								},
							},
						},
					},
				},
			}
		}
		events = append(events, es)
	}

	if err := txn.Upsert(jobsToUpdate); err != nil {
		return nil, err
	}
	return events, nil
}

// now is a convenience function for generating a proto timestamp representing the current time according to the clock
func (s *Scheduler) now() *types.Timestamp {
	return protoutil.ToTimestamp(s.clock.Now())
}

// initialise builds the initial job db based on the current database state
// right now this is quite dim and loads the entire database but in the future
// we should be  able to make it load active jobs/runs only
func (s *Scheduler) initialise(ctx *armadacontext.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if _, _, err := s.syncState(ctx, true); err != nil {
				ctx.Logger().
					WithStacktrace(err).
					Error("failed to initialise; trying again in 1 second")
				time.Sleep(1 * time.Second)
			} else {
				ctx.Info("initialisation succeeded")
				return nil
			}
		}
	}
}

// ensureDbUpToDate blocks until that the database state contains all Pulsar messages sent *before* this
// function was called. This is achieved firstly by publishing messages to Pulsar and then polling the
// database until all messages have been written.
func (s *Scheduler) ensureDbUpToDate(ctx *armadacontext.Context, pollInterval time.Duration) error {
	groupId := uuid.New()
	var numSent uint32
	var err error

	// Send messages to Pulsar
	messagesSent := false
	for !messagesSent {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			numSent, err = s.publisher.PublishMarkers(ctx, groupId)
			if err != nil {
				ctx.Logger().
					WithStacktrace(err).
					Error("Error sending marker messages to pulsar")
				s.clock.Sleep(pollInterval)
			} else {
				messagesSent = true
			}
		}
	}

	// Try to read these messages back from postgres.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			numReceived, err := s.jobRepository.CountReceivedPartitions(ctx, groupId)
			if err != nil {
				ctx.Logger().
					WithStacktrace(err).
					Error("Error querying the database or marker messages")
			}
			if numSent == numReceived {
				ctx.Infof("Successfully ensured that database state is up to date")
				return nil
			}
			ctx.Infof("Received %d partitions, still waiting on %d", numReceived, numSent-numReceived)
			s.clock.Sleep(pollInterval)
		}
	}
}
