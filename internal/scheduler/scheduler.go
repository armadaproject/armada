package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/affinity"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Scheduler is the main Armada scheduler.
// It periodically performs the following cycle:
// 1. Update state from postgres (via the jobRepository).
// 2. Determine if leader and exit if not.
// 3. Generate any necessary events resulting from the state update.
// 4. Expire any jobs assigned to clusters that have timed out.
// 5. Schedule jobs.
// 6. Publish any Armada events resulting from the scheduling cycle.
type Scheduler struct {
	// Provides job updates from Postgres.
	jobRepository database.JobRepository
	// Used to determine whether a cluster is active.
	executorRepository database.ExecutorRepository
	// Responsible for assigning jobs to nodes.
	// TODO: Confusing name. Change.
	schedulingAlgo SchedulingAlgo
	// Tells us if we are leader. Only the leader may schedule jobs.
	leaderController LeaderController
	// We intern strings to save memory.
	stringInterner *stringinterner.StringInterner
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
	// all jobs assigne to that executor are cancelled.
	executorTimeout time.Duration
	// The time the previous scheduling round ended
	previousSchedulingRoundEnd time.Time
	// Used for timing decisions (e.g., sleep).
	// Injected here so that we can mock it out for testing.
	clock clock.Clock
	// Stores active jobs (i.e. queued or running).
	jobDb *jobdb.JobDb
	// Highest offset we've read from Postgres on the jobs table.
	jobsSerial int64
	// Highest offset we've read from Postgres on the job runs table.
	runsSerial int64
	// Function that is called every time a cycle is completed. Useful for testing.
	onCycleCompleted func()
}

func NewScheduler(
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	schedulingAlgo SchedulingAlgo,
	leaderController LeaderController,
	publisher Publisher,
	stringInterner *stringinterner.StringInterner,
	submitChecker SubmitScheduleChecker,
	cyclePeriod time.Duration,
	schedulePeriod time.Duration,
	executorTimeout time.Duration,
	maxAttemptedRuns uint,
	nodeIdLabel string,
) (*Scheduler, error) {
	jobDb := jobdb.NewJobDb()
	return &Scheduler{
		jobRepository:              jobRepository,
		executorRepository:         executorRepository,
		schedulingAlgo:             schedulingAlgo,
		leaderController:           leaderController,
		publisher:                  publisher,
		stringInterner:             stringInterner,
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
	}, nil
}

// Run enters the scheduling loop, which will continue until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("service", "scheduler")
	ctx = ctxlogrus.ToContext(ctx, log)
	log.Infof("starting scheduler with cycle time %s", s.cyclePeriod)
	defer log.Info("scheduler stopped")

	// JobDb initialisation.
	start := s.clock.Now()
	if err := s.initialise(ctx); err != nil {
		return err
	}
	log.Infof("JobDb initialised in %s", s.clock.Since(start))

	ticker := s.clock.NewTicker(s.cyclePeriod)
	prevLeaderToken := InvalidLeaderToken()
	for {
		select {
		case <-ctx.Done():
			log.Infof("context cancelled; returning.")
			return ctx.Err()
		case <-ticker.C():
			start := s.clock.Now()
			leaderToken := s.leaderController.GetToken()
			fullUpdate := false
			log.Infof("received leaderToken; leader status is %t", leaderToken.leader)

			// If we are becoming leader then we must ensure we have caught up to all Pulsar messages
			if leaderToken.leader && leaderToken != prevLeaderToken {
				log.Infof("becoming leader")
				syncContext, cancel := context.WithTimeout(ctx, 5*time.Minute)
				err := s.ensureDbUpToDate(syncContext, 1*time.Second)
				if err != nil {
					log.WithError(err).Error("could not become leader")
					leaderToken = InvalidLeaderToken()
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
			if err := s.cycle(ctx, fullUpdate, leaderToken); err != nil {
				logging.WithStacktrace(log, err).Error("scheduling cycle failure")
				leaderToken = InvalidLeaderToken()
			}
			log.Infof("scheduling cycle completed in %s", s.clock.Since(start))
			prevLeaderToken = leaderToken
			if s.onCycleCompleted != nil {
				s.onCycleCompleted()
			}
		}
	}
}

// cycle is a single iteration of the main scheduling loop.
// If updateAll is true, we generate events from all jobs in the jobDb.
// Otherwise, we only generate events from jobs updated since the last cycle.
func (s *Scheduler) cycle(ctx context.Context, updateAll bool, leaderToken LeaderToken) error {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("function", "cycle")
	// Update job state.
	updatedJobs, err := s.syncState(ctx)
	if err != nil {
		return err
	}

	// Only the leader may make decisions; exit if not leader.
	if !s.leaderController.ValidateToken(leaderToken) {
		return nil
	}

	// If we've been asked to generate messages for all jobs, do so.
	// Otherwise generate messages only for jobs updated this cycle.
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()
	if updateAll {
		updatedJobs = s.jobDb.GetAll(txn)
	}

	// Generate any events that came out of synchronising the db state.
	events, err := s.generateUpdateMessages(ctx, updatedJobs, txn)
	if err != nil {
		return err
	}

	// Expire any jobs running on clusters that haven't heartbeated within the configured deadline.
	expirationEvents, err := s.expireJobsIfNecessary(ctx, txn)
	if err != nil {
		return err
	}
	events = append(events, expirationEvents...)

	// Schedule jobs.
	if s.clock.Now().Sub(s.previousSchedulingRoundEnd) > s.schedulePeriod {
		overallSchedulerResult, err := s.schedulingAlgo.Schedule(ctx, txn, s.jobDb)
		if err != nil {
			return err
		}

		resultEvents, err := s.eventsFromSchedulerResult(txn, overallSchedulerResult)
		if err != nil {
			return err
		}
		events = append(events, resultEvents...)
		s.previousSchedulingRoundEnd = s.clock.Now()
	}

	// Publish to Pulsar.
	isLeader := func() bool {
		return s.leaderController.ValidateToken(leaderToken)
	}
	if err := s.publisher.PublishMessages(ctx, events, isLeader); err != nil {
		return err
	}
	txn.Commit()
	return nil
}

// syncState updates jobs in jobDb to match state in postgres and returns all updated jobs.
func (s *Scheduler) syncState(ctx context.Context) ([]*jobdb.Job, error) {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("function", "syncState")

	updatedJobs, updatedRuns, err := s.jobRepository.FetchJobUpdates(ctx, s.jobsSerial, s.runsSerial)
	if err != nil {
		return nil, err
	}
	log.Infof("received %d updated jobs and %d updated job runs", len(updatedJobs), len(updatedRuns))

	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	// Process jobs.
	jobsToDelete := make([]string, 0, len(updatedJobs))
	jobsToUpdateById := make(map[string]*jobdb.Job, len(updatedJobs))
	for _, dbJob := range updatedJobs {
		if dbJob.InTerminalState() {
			// Scheduler has sent a terminal message; we can safely remove the job.
			jobsToDelete = append(jobsToDelete, dbJob.JobID)
			continue
		}

		// Try and retrieve the job from the jobDb. If it doesn't exist then create it.
		job := s.jobDb.GetById(txn, dbJob.JobID)
		if job == nil {
			job, err = s.schedulerJobFromDatabaseJob(&dbJob)
			if err != nil {
				return nil, err
			}
		} else {
			// make the scheduler job look like the db job.
			job, err = updateSchedulerJob(job, &dbJob)
			if err != nil {
				return nil, err
			}
		}
		jobsToUpdateById[job.Id()] = job
	}

	// Process runs.
	for _, dbRun := range updatedRuns {
		jobId := dbRun.JobID

		// Retrieve the job, look first in the list of updates, then in the jobDb.
		job, present := jobsToUpdateById[jobId]
		if !present {
			job = s.jobDb.GetById(txn, jobId)

			// If the job is nil or terminal at this point then it cannot be active.
			// In this case we can ignore the run.
			if job == nil || job.InTerminalState() {
				log.Debugf("job %s is not active; ignoring update for run %s", jobId, dbRun.RunID)
				continue
			}
		}

		run := job.RunById(dbRun.RunID)
		if run == nil {
			run = s.createSchedulerRun(&dbRun)
		} else {
			// make the scheduler job look like the db job
			run = updateSchedulerRun(run, &dbRun)
		}
		job = job.WithUpdatedRun(run)
		jobsToUpdateById[jobId] = job
	}

	jobsToUpdate := maps.Values(jobsToUpdateById)
	err = s.jobDb.BatchDelete(txn, jobsToDelete)
	if err != nil {
		return nil, err
	}
	err = s.jobDb.Upsert(txn, jobsToUpdate)
	if err != nil {
		return nil, err
	}
	txn.Commit()
	if len(updatedJobs) > 0 {
		s.jobsSerial = updatedJobs[len(updatedJobs)-1].Serial
	}
	if len(updatedRuns) > 0 {
		s.runsSerial = updatedRuns[len(updatedRuns)-1].Serial
	}
	return jobsToUpdate, nil
}

func (s *Scheduler) createSchedulingInfoWithNodeAntiAffinityForAttemptedRuns(job *jobdb.Job) (*schedulerobjects.JobSchedulingInfo, error) {
	newSchedulingInfo := proto.Clone(job.JobSchedulingInfo()).(*schedulerobjects.JobSchedulingInfo)
	newSchedulingInfo.Version = job.JobSchedulingInfo().Version + 1
	podRequirements := newSchedulingInfo.GetPodRequirements()
	if podRequirements == nil {
		return nil, errors.Errorf("no pod scheduling requirement found for job %s", job.GetId())
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

func (s *Scheduler) addNodeAntiAffinitiesForAttemptedRunsIfSchedulable(job *jobdb.Job) (*jobdb.Job, bool, error) {
	schedulingInfoWithNodeAntiAffinity, err := s.createSchedulingInfoWithNodeAntiAffinityForAttemptedRuns(job)
	if err != nil {
		return nil, false, err
	}
	job = job.WithJobSchedulingInfo(schedulingInfoWithNodeAntiAffinity)
	isSchedulable, _ := s.submitChecker.CheckJobDbJobs([]*jobdb.Job{job})
	return job, isSchedulable, nil
}

// eventsFromSchedulerResult generates necessary EventSequences from the provided SchedulerResult.
func (s *Scheduler) eventsFromSchedulerResult(txn *jobdb.Txn, result *SchedulerResult) ([]*armadaevents.EventSequence, error) {
	events := make([]*armadaevents.EventSequence, 0, len(result.PreemptedJobs)+len(result.ScheduledJobs))
	for _, job := range PreemptedJobsFromSchedulerResult[*jobdb.Job](result) {
		jobId, err := armadaevents.ProtoUuidFromUlidString(job.Id())
		if err != nil {
			return nil, err
		}
		run := job.LatestRun()
		if run == nil {
			return nil, errors.Errorf("attempting to generate preempted events for job %s with no associated runs", job.Id())
		}
		es := &armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobRunPreempted{
						JobRunPreempted: &armadaevents.JobRunPreempted{
							PreemptedRunId: armadaevents.ProtoUuidFromUuid(run.Id()),
							PreemptedJobId: jobId,
						},
					},
				},
				{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobRunErrors{
						JobRunErrors: &armadaevents.JobRunErrors{
							RunId: armadaevents.ProtoUuidFromUuid(run.Id()),
							JobId: jobId,
							Errors: []*armadaevents.Error{
								{
									Terminal: true,
									Reason: &armadaevents.Error_JobRunPreemptedError{
										JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
									},
								},
							},
						},
					},
				},
				{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobErrors{
						JobErrors: &armadaevents.JobErrors{
							JobId: jobId,
							Errors: []*armadaevents.Error{
								{
									Terminal: true,
									Reason: &armadaevents.Error_JobRunPreemptedError{
										JobRunPreemptedError: &armadaevents.JobRunPreemptedError{},
									},
								},
							},
						},
					},
				},
			},
		}
		events = append(events, es)
	}
	for _, job := range ScheduledJobsFromSchedulerResult[*jobdb.Job](result) {
		jobId, err := armadaevents.ProtoUuidFromUlidString(job.Id())
		if err != nil {
			return nil, err
		}
		job = job.WithQueuedVersion(job.QueuedVersion() + 1)
		job = job.WithQueued(false)
		err = s.jobDb.Upsert(txn, []*jobdb.Job{job})
		if err != nil {
			return nil, err
		}
		events = append(
			events,
			&armadaevents.EventSequence{
				Queue:      job.Queue(),
				JobSetName: job.Jobset(), // TODO: Rename to JobSet.
				Events: []*armadaevents.EventSequence_Event{
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobRunLeased{
							JobRunLeased: &armadaevents.JobRunLeased{
								RunId:      armadaevents.ProtoUuidFromUuid(job.LatestRun().Id()),
								JobId:      jobId,
								ExecutorId: job.LatestRun().Executor(),
								// NodeId here refers to the unique identifier of the node in an executor cluster,
								// which is referred to as the NodeName within the scheduler.
								NodeId:               job.LatestRun().NodeName(),
								UpdateSequenceNumber: job.QueuedVersion(),
							},
						},
					},
				},
			},
		)
	}

	return events, nil
}

// generateUpdateMessages generates EventSequences representing the state changes on updated jobs
// If there are no state changes then an empty slice will be returned
func (s *Scheduler) generateUpdateMessages(ctx context.Context, updatedJobs []*jobdb.Job, txn *jobdb.Txn) ([]*armadaevents.EventSequence, error) {
	failedRunIds := make([]uuid.UUID, 0, len(updatedJobs))
	for _, job := range updatedJobs {
		run := job.LatestRun()
		if run != nil && run.Failed() {
			failedRunIds = append(failedRunIds, run.Id())
		}
	}
	jobRunErrors, err := s.jobRepository.FetchJobRunErrors(ctx, failedRunIds)
	if err != nil {
		return nil, err
	}

	// Generate any events that came out of synchronising the db state
	var events []*armadaevents.EventSequence
	for _, job := range updatedJobs {
		jobEvents, err := s.generateUpdateMessagesFromJob(job, jobRunErrors, txn)
		if err != nil {
			return nil, err
		}
		if jobEvents != nil {
			events = append(events, jobEvents)
		}
	}
	return events, nil
}

// generateUpdateMessages generates EventSequence representing the state change on a single jobs
// If there are no state changes then nil will be returned
func (s *Scheduler) generateUpdateMessagesFromJob(job *jobdb.Job, jobRunErrors map[uuid.UUID]*armadaevents.Error, txn *jobdb.Txn) (*armadaevents.EventSequence, error) {
	var events []*armadaevents.EventSequence_Event

	// Is the job already in a terminal state?  If so then don't send any more messages
	if job.InTerminalState() {
		return nil, nil
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(job.Id())
	if err != nil {
		return nil, err
	}
	origJob := job
	// Has the job been requested cancelled. If so, cancel the job
	if job.CancelRequested() {
		for _, run := range job.AllRuns() {
			job = job.WithUpdatedRun(run.WithCancelled(true))
		}
		job = job.WithCancelled(true).WithQueued(false)
		cancel := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{JobId: jobId},
			},
		}
		events = append(events, cancel)
	} else if job.CancelByJobsetRequested() {
		for _, run := range job.AllRuns() {
			job = job.WithUpdatedRun(run.WithCancelled(true))
		}
		job = job.WithCancelled(true).WithQueued(false)
		cancelRequest := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{JobId: jobId},
			},
		}
		cancel := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{JobId: jobId},
			},
		}
		events = append(events, cancelRequest, cancel)
	} else if job.HasRuns() {
		lastRun := job.LatestRun()
		// InTerminalState states. Can only have one of these
		if lastRun.Succeeded() {
			job = job.WithSucceeded(true).WithQueued(false)
			jobSucceeded := &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobSucceeded{
					JobSucceeded: &armadaevents.JobSucceeded{
						JobId: jobId,
					},
				},
			}
			events = append(events, jobSucceeded)
		} else if lastRun.Failed() && !job.Queued() {
			requeueJob := lastRun.Returned() && job.NumAttempts() < s.maxAttemptedRuns

			if requeueJob && lastRun.RunAttempted() {
				jobWithAntiAffinity, schedulable, err := s.addNodeAntiAffinitiesForAttemptedRunsIfSchedulable(job)
				if err != nil {
					return nil, errors.Errorf("unable to set node anti-affinity for job %s because %s", job.GetId(), err)
				} else {
					if schedulable {
						job = jobWithAntiAffinity
					} else {
						// If job is not schedulable with anti-affinity added. Do not requeue it and let it fail
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
							JobId:                jobId,
							SchedulingInfo:       job.JobSchedulingInfo(),
							UpdateSequenceNumber: job.QueuedVersion(),
						},
					},
				}

				events = append(events, requeueJobEvent)
			} else {
				runError := jobRunErrors[lastRun.Id()]
				job = job.WithFailed(true).WithQueued(false)
				if lastRun.Returned() {
					errorMessage := fmt.Sprintf("Maximum number of attempts (%d) reached - this job will no longer be retried", s.maxAttemptedRuns)
					if job.NumAttempts() < s.maxAttemptedRuns {
						errorMessage = fmt.Sprintf("Job was attempted %d times, and has been tried once on all nodes it can run on - this job will no longer be retried", job.NumAttempts())
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
							JobId:  jobId,
							Errors: []*armadaevents.Error{runError},
						},
					},
				}

				events = append(events, jobErrors)
			}
		}
	} else if job.RequestedPriority() != job.Priority() {
		job = job.WithPriority(job.RequestedPriority())
		jobReprioritised := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_ReprioritisedJob{
				ReprioritisedJob: &armadaevents.ReprioritisedJob{
					JobId:    jobId,
					Priority: job.Priority(),
				},
			},
		}
		events = append(events, jobReprioritised)
	}

	if origJob != job {
		err := s.jobDb.Upsert(txn, []*jobdb.Job{job})
		if err != nil {
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
func (s *Scheduler) expireJobsIfNecessary(ctx context.Context, txn *jobdb.Txn) ([]*armadaevents.EventSequence, error) {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("function", "expireJobsIfNecessary")

	heartbeatTimes, err := s.executorRepository.GetLastUpdateTimes(ctx)
	if err != nil {
		return nil, err
	}
	staleExecutors := make(map[string]bool, 0)
	cutOff := s.clock.Now().Add(-s.executorTimeout)

	jobsToUpdate := make([]*jobdb.Job, 0)

	// TODO: this will only detect stale clusters if they exist in the database
	// Right now this is fine because nothing will delete this jobs, but we should consider the case where an executor
	// has been completely removed
	for executor, heartbeat := range heartbeatTimes {
		if heartbeat.Before(cutOff) {
			log.Warnf("Executor %s has not reported a hearbeart since %v. Will expire all jobs running on this executor", executor, heartbeat)
			staleExecutors[executor] = true
		}
	}

	// All clusters have had a heartbeat recently.  No need to expire any jobs
	if len(staleExecutors) == 0 {
		log.Infof("No stale executors found. No jobs need to be expired")
		return nil, nil
	}

	events := make([]*armadaevents.EventSequence, 0)

	// TODO: this is inefficient.  We should create a iterator of the jobs running on the affected executors
	jobs := s.jobDb.GetAll(txn)

	for _, job := range jobs {

		if job.InTerminalState() {
			continue
		}

		run := job.LatestRun()
		if run != nil && !job.Queued() && staleExecutors[run.Executor()] {
			log.Warnf("Cancelling job %s as it is running on lost executor %s", job.Id(), run.Executor())
			jobsToUpdate = append(jobsToUpdate, job.WithQueued(false).WithFailed(true).WithUpdatedRun(run.WithFailed(true)))

			jobId, err := armadaevents.ProtoUuidFromUlidString(job.Id())
			if err != nil {
				return nil, err
			}

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
								RunId:  armadaevents.ProtoUuidFromUuid(run.Id()),
								JobId:  jobId,
								Errors: []*armadaevents.Error{leaseExpiredError},
							},
						},
					},
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobErrors{
							JobErrors: &armadaevents.JobErrors{
								JobId:  jobId,
								Errors: []*armadaevents.Error{leaseExpiredError},
							},
						},
					},
				},
			}
			events = append(events, es)
		}
	}
	if err := s.jobDb.Upsert(txn, jobsToUpdate); err != nil {
		return nil, err
	}
	return events, nil
}

// now is a convenience function for generating a pointer to a time.Time (as required by armadaevents).
// It exists because Go won't let you do &s.clock.Now().
func (s *Scheduler) now() *time.Time {
	now := s.clock.Now()
	return &now
}

// initialise builds the initial job db based on the current database state
// right now this is quite dim and loads the entire database but in the future
// we should be  able to make it load active jobs/runs only
func (s *Scheduler) initialise(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("function", "initialise")
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if _, err := s.syncState(ctx); err != nil {
				log.WithError(err).Error("failed to initialise; trying again in 1 second")
				time.Sleep(1 * time.Second)
			} else {
				// Initialisation succeeded.
				return nil
			}
		}
	}
}

// ensureDbUpToDate blocks until that the database state contains all Pulsar messages sent *before* this
// function was called. This is achieved firstly by publishing messages to Pulsar and then polling the
// database until all messages have been written.
func (s *Scheduler) ensureDbUpToDate(ctx context.Context, pollInterval time.Duration) error {
	log := ctxlogrus.Extract(ctx)
	log = log.WithField("function", "ensureDbUpToDate")

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
				log.WithError(err).Error("Error sending marker messages to pulsar")
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
				log.WithError(err).Error("Error querying the database or marker messages")
			}
			if numSent == numReceived {
				log.Infof("Successfully ensured that database state is up to date")
				return nil
			}
			log.Infof("Recevied %d partitions, still waiting on  %d", numReceived, numSent-numReceived)
			s.clock.Sleep(pollInterval)
		}
	}
}

// schedulerJobFromDatabaseJob creates a new scheduler job from a database job.
func (s *Scheduler) schedulerJobFromDatabaseJob(dbJob *database.Job) (*jobdb.Job, error) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
	err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo)
	if err != nil {
		return nil, errors.Wrapf(err, "error unmarshalling scheduling info for job %s", dbJob.JobID)
	}
	s.internJobSchedulingInfoStrings(schedulingInfo)
	return jobdb.NewJob(
		dbJob.JobID,
		s.stringInterner.Intern(dbJob.JobSet),
		s.stringInterner.Intern(dbJob.Queue),
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

// createSchedulerRun creates a new scheduler job run from a database job run
func (s *Scheduler) createSchedulerRun(dbRun *database.Run) *jobdb.JobRun {
	nodeId := api.NodeIdFromExecutorAndNodeName(dbRun.Executor, dbRun.Node)
	return jobdb.CreateRun(
		dbRun.RunID,
		dbRun.JobID,
		dbRun.Created,
		s.stringInterner.Intern(dbRun.Executor),
		s.stringInterner.Intern(nodeId),
		s.stringInterner.Intern(dbRun.Node),
		dbRun.Running,
		dbRun.Succeeded,
		dbRun.Failed,
		dbRun.Cancelled,
		dbRun.Returned,
		dbRun.RunAttempted,
	)
}

func (s *Scheduler) internJobSchedulingInfoStrings(info *schedulerobjects.JobSchedulingInfo) {
	for _, requirement := range info.ObjectRequirements {
		if podRequirement := requirement.GetPodRequirements(); podRequirement != nil {
			for k, v := range podRequirement.Annotations {
				podRequirement.Annotations[s.stringInterner.Intern(k)] = s.stringInterner.Intern(v)
			}

			for k, v := range podRequirement.NodeSelector {
				podRequirement.NodeSelector[s.stringInterner.Intern(k)] = s.stringInterner.Intern(v)
			}
			podRequirement.PreemptionPolicy = s.stringInterner.Intern(podRequirement.PreemptionPolicy)
		}
	}
}

// updateSchedulerRun updates the scheduler job run (in-place) to match the database job run
func updateSchedulerRun(run *jobdb.JobRun, dbRun *database.Run) *jobdb.JobRun {
	if dbRun.Succeeded && !run.Succeeded() {
		run = run.WithSucceeded(true)
	}
	if dbRun.Failed && !run.Failed() {
		run = run.WithFailed(true)
	}
	if dbRun.Cancelled && !run.Cancelled() {
		run = run.WithCancelled(true)
	}
	if dbRun.Returned && !run.Returned() {
		run = run.WithReturned(true)
	}
	if dbRun.RunAttempted && !run.RunAttempted() {
		run = run.WithAttempted(true)
	}
	return run
}

// updateSchedulerJob updates the scheduler job in-place to match the database job.
func updateSchedulerJob(job *jobdb.Job, dbJob *database.Job) (*jobdb.Job, error) {
	if dbJob.CancelRequested && !job.CancelRequested() {
		job = job.WithCancelRequested(true)
	}
	if dbJob.CancelByJobsetRequested && !job.CancelByJobsetRequested() {
		job = job.WithCancelByJobsetRequested(true)
	}
	if dbJob.Cancelled && !job.Cancelled() {
		job = job.WithCancelled(true)
	}
	if dbJob.Succeeded && !job.Succeeded() {
		job = job.WithSucceeded(true)
	}
	if dbJob.Failed && !job.Failed() {
		job = job.WithFailed(true)
	}
	if uint32(dbJob.Priority) != job.RequestedPriority() {
		job = job.WithRequestedPriority(uint32(dbJob.Priority))
	}
	if uint32(dbJob.SchedulingInfoVersion) > job.JobSchedulingInfo().Version {
		schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
		err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo)
		if err != nil {
			return nil, errors.Wrapf(err, "error unmarshalling scheduling info for job %s", dbJob.JobID)
		}
		job = job.WithJobSchedulingInfo(schedulingInfo)
	}
	if dbJob.QueuedVersion > job.QueuedVersion() {
		job = job.WithQueuedVersion(dbJob.QueuedVersion)
		job = job.WithQueued(dbJob.Queued)
	}
	return job, nil
}
