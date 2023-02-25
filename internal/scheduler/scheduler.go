package scheduler

import (
	"context"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Scheduler is the main armada Scheduler. It runs a periodic scheduling cycle during which the following actions are
// performed:
// * Determine if we are leader
// * Update internal state from postgres (via the jobRepository)
// * If Leader:
//   - Generate any armada events resulting from the state update
//   - Expire any jobs that are running on  stale clusters
//   - Attempt to schedule jobs from the queue
//   - Publish any armada events resulting from the cycle to Pulsar
type Scheduler struct {
	// Provides job updates from Postgres
	jobRepository database.JobRepository
	// Used to determine whether a cluster is active
	executorRepository database.ExecutorRepository
	// Responsible for assigning jobs to nodes
	schedulingAlgo SchedulingAlgo
	// Tells us if we are leader. Only the leader may schedule jobs
	leaderController LeaderController
	// We intern strings to save memory
	stringInterner *util.StringInterner
	// Responsible for publishing messages to Pulsar.  Only the leader publishes.
	publisher Publisher
	// Minimum duration between scheduling cycles.
	cyclePeriod time.Duration
	// Maximum number of times a lease can be returned before the job is considered failed
	maxLeaseReturns uint
	// ClusterTimeout
	executorTimeout time.Duration
	// Used for all timing decisions (sleep etc.). Injected here so that we can mock out for testing
	clock clock.Clock
	// Stores active jobs (i.e. queued/running) and provides fast in-memory lookups on them
	jobDb *jobdb.JobDb
	// Highest offset we've read from Postgres on the Jobs table.
	jobsSerial int64
	// Highest offset we've read from Postgres on the Job Runs table.
	runsSerial int64
	// Function that is called every time a cycle is completed.  Useful for testing
	onCycleCompleted func()
}

func NewScheduler(
	jobRepository database.JobRepository,
	executorRepository database.ExecutorRepository,
	schedulingAlgo SchedulingAlgo,
	leaderController LeaderController,
	publisher Publisher,
	stringInterner *util.StringInterner,
	cyclePeriod time.Duration,
	executorTimeout time.Duration,
	maxLeaseReturns uint,
) (*Scheduler, error) {
	jobDb := jobdb.NewJobDb()
	return &Scheduler{
		jobRepository:      jobRepository,
		executorRepository: executorRepository,
		schedulingAlgo:     schedulingAlgo,
		leaderController:   leaderController,
		publisher:          publisher,
		stringInterner:     stringInterner,
		jobDb:              jobDb,
		clock:              clock.RealClock{},
		cyclePeriod:        cyclePeriod,
		executorTimeout:    executorTimeout,
		maxLeaseReturns:    maxLeaseReturns,
		jobsSerial:         -1,
		runsSerial:         -1,
	}, nil
}

// Run enters the scheduling loop, which will continue until the ctx is cancelled
func (s *Scheduler) Run(ctx context.Context) error {
	//  Do initial population of Job Db
	err := s.initialise(ctx)
	if err != nil {
		return err
	}

	ticker := s.clock.NewTicker(s.cyclePeriod)
	log.Infof("Will run scheduling cycle every %s", s.cyclePeriod)
	prevLeaderToken := LeaderToken{leader: false, id: uuid.New()}
	for {
		select {
		case <-ctx.Done():
			log.Infof("Context cancelled, returning..")
			return nil
		case <-ticker.C():
			start := s.clock.Now()
			leaderToken := s.leaderController.GetToken()
			fullUpdate := false
			log.Infof("Received a leaderToken. Leader status is %t", leaderToken.leader)

			// If we are becoming leader then we must ensure we have caught up to all Pulsar messages
			if leaderToken.leader && leaderToken != prevLeaderToken {
				log.Infof("Becoming leader")
				syncContext, cancel := context.WithTimeout(ctx, 5*time.Minute)
				err := s.ensureDbUpToDate(syncContext, 1*time.Second)
				if err != nil {
					log.WithError(err).Error("Could not become master")
					leaderToken = InvalidLeaderToken()
				} else {
					fullUpdate = true
				}
				cancel()
			}
			// Run a scheduler cycle
			err := s.cycle(ctx, fullUpdate, leaderToken)
			// If the scheduler cycle has returned an error, we must invalidate our leader token
			// This is because right now we cannot use pulsar transactions which in turn means the possibility of
			// a partial publish and consequently an inconsistent state.  Once the Pulsar client supports transactions
			// we should be able to remove this limitation
			if err != nil {
				log.WithError(err).Error("Error in scheduling cycle")
				leaderToken = InvalidLeaderToken()
			}
			taken := s.clock.Now().Sub(start)
			log.Infof("Completed scheduling cycle in %s", taken)
			prevLeaderToken = leaderToken
			if s.onCycleCompleted != nil {
				s.onCycleCompleted()
			}
		}
	}
}

// cycle is a single invocation of the main scheduling cycle
// if updateAll is true then we will generate armada events from all jobs in the jobDb, else we only
// generate updates from those jobs which have been updated since the last cycle
func (s *Scheduler) cycle(ctx context.Context, updateAll bool, leaderToken LeaderToken) error {
	// Update job state
	updatedJobs, err := s.syncState(ctx)
	if err != nil {
		return err
	}

	// Am I leader? If not then we're done
	if !s.leaderController.ValidateToken(leaderToken) {
		return nil
	}

	// Start a transaction- we'll roll this back if we don't publish
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	// If we've been asked to generate messages for all jobs do so, else generate messages only for jobs updated this
	// cycle
	if updateAll {
		updatedJobs = s.jobDb.GetAll(txn)
	}

	// Generate any events that came out of synchronising the db state
	events, err := s.generateUpdateMessages(ctx, updatedJobs, txn)
	if err != nil {
		return err
	}

	// Expire any jobs running on clusters that haven't heartbeated within our time limit
	expirationEvents, err := s.expireJobsIfNecessary(ctx, txn)
	if err != nil {
		return err
	}
	events = append(events, expirationEvents...)

	// Schedule Jobs
	scheduledJobs, err := s.schedulingAlgo.Schedule(ctx, txn, s.jobDb)
	if err != nil {
		return err
	}
	scheduledJobEvents, err := s.generateLeaseMessages(scheduledJobs)
	if err != nil {
		return err
	}
	events = append(events, scheduledJobEvents...)

	// Publish to pulsar
	amLeader := func() bool {
		return s.leaderController.ValidateToken(leaderToken)
	}
	err = s.publisher.PublishMessages(ctx, events, amLeader)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

// syncState updates the state of the jobs in jobDb to match the state in postgres
// It returns all jobs that have been updated
func (s *Scheduler) syncState(ctx context.Context) ([]*jobdb.Job, error) {
	updatedJobs, updatedRuns, err := s.jobRepository.FetchJobUpdates(ctx, s.jobsSerial, s.runsSerial)
	if err != nil {
		return nil, err
	}
	log.Infof("Received %d updated jobs and %d updated job runs", len(updatedJobs), len(updatedRuns))

	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	jobsToDelete := make([]string, 0, len(updatedJobs))
	jobsToUpdateById := make(map[string]*jobdb.Job, len(updatedJobs))
	for _, dbJob := range updatedJobs {
		// Scheduler has sent a terminal message therefore we can safely remove the job
		if dbJob.InTerminalState() {
			jobsToDelete = append(jobsToDelete, dbJob.JobID)
			continue
		}

		// Try and retrieve the job from the jobDb.  If it doesn't exist then create it.
		job := s.jobDb.GetById(txn, dbJob.JobID)
		if job == nil {
			job, err = s.createSchedulerJob(&dbJob)
			if err != nil {
				return nil, err
			}
		} else {
			// make the scheduler job look like the db job
			job = updateSchedulerJob(job, &dbJob)
		}
		jobsToUpdateById[job.Id()] = job
	}

	for _, dbRun := range updatedRuns {
		jobId := dbRun.JobID

		// Retrieve the job, look first in the list of updates, then in the jobDb
		job, present := jobsToUpdateById[jobId]
		if !present {
			job = s.jobDb.GetById(txn, jobId)

			// If the job is nil or terminal at this point then it cannot be active.
			// In this case we can ignore the run
			if job == nil || job.InTerminalState() {
				log.Debugf("Job %s is not an active job. Ignoring update for run %s", jobId, dbRun.RunID)
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

	// any jobs that have don't have active run need to be marked as queued
	for _, job := range jobsToUpdateById {
		// work out if the job needs to be re-queued.  This is a bit awkward as the old scheduler
		// didn't send an explicit queued message here which means we have to infer it. For now, we
		// do the same, but eventually we should send an actual queued message and this bit of code can disappear
		run := job.LatestRun()
		desiredQueueState := false
		requeueJob := run != nil && run.Returned() && job.NumReturned() <= s.maxLeaseReturns
		if run == nil || requeueJob {
			desiredQueueState = true
		}
		if requeueJob {
			job = job.WithUpdatedRun(run.WithFailed(false))
		}
		if desiredQueueState != job.Queued() || requeueJob {
			jobsToUpdateById[job.Id()] = job.WithQueued(desiredQueueState)
		}
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

// generateLeaseMessages generates EventSequences from the supplied slice of leased jobs
func (s *Scheduler) generateLeaseMessages(scheduledJobs []*jobdb.Job) ([]*armadaevents.EventSequence, error) {
	events := make([]*armadaevents.EventSequence, len(scheduledJobs))
	for i, job := range scheduledJobs {
		jobId, err := armadaevents.ProtoUuidFromUlidString(job.Id())
		if err != nil {
			return nil, err
		}
		es := &armadaevents.EventSequence{
			Queue:      job.Queue(),
			JobSetName: job.Jobset(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobRunLeased{
						JobRunLeased: &armadaevents.JobRunLeased{
							RunId:      armadaevents.ProtoUuidFromUuid(job.LatestRun().Id()),
							JobId:      jobId,
							ExecutorId: job.LatestRun().Executor(),
						},
					},
				},
			},
		}
		events[i] = es
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
		job = job.WithCancelled(true).WithQueued(false)
		cancel := &armadaevents.EventSequence_Event{
			Created: s.now(),
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{JobId: jobId},
			},
		}
		events = append(events, cancel)
	} else if job.CancelByJobsetRequested() {
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
		} else if lastRun.Failed() {
			job = job.WithFailed(true).WithQueued(false)
			runError := jobRunErrors[lastRun.Id()]
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
			jobsToUpdate = append(jobsToUpdate, job.WithQueued(false).WithFailed(true))

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
	err = s.jobDb.Upsert(txn, jobsToUpdate)
	if err != nil {
		return nil, err
	}
	return events, nil
}

// now is a convenience function for generating a pointer to  a time.Time (as required by armadaevents).  It exists
// because Go won't let you do &s.clock.Now()
func (s *Scheduler) now() *time.Time {
	now := s.clock.Now()
	return &now
}

// initialise builds the initial job db based on the current database state
// right now this is quite dim and loads the entire database but in the future
// we should be  able to make it load active jobs/runs only
func (s *Scheduler) initialise(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := s.syncState(ctx)
			if err == nil {
				return nil
			}
			log.WithError(err).Error("Error initialising. Sleeping for 1 second before trying again")
			time.Sleep(1 * time.Second)
		}
	}
}

// ensureDbUpToDate  blocks until that the database state contains all Pulsar messages sent *before* this
// function was called. This is achieved firstly by publishing messages to Pulsar and then polling the
// database until all messages have been written.
func (s *Scheduler) ensureDbUpToDate(ctx context.Context, pollInterval time.Duration) error {
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

	// Try to read these messages back from the DB
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			numReceived, err := s.jobRepository.CountReceivedPartitions(ctx, groupId)
			if err != nil {
				log.WithError(err).Error("Error querying the database  or marker messages")
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

// createSchedulerJob creates a new scheduler job from a database job
func (s *Scheduler) createSchedulerJob(dbJob *database.Job) (*jobdb.Job, error) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
	err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo)
	if err != nil {
		return nil, errors.Wrapf(
			errors.WithStack(err), "error unmarshalling scheduling info for job %s", dbJob.JobID)
	}
	s.internJobSchedulingInfoStrings(schedulingInfo)
	return jobdb.NewJob(
		dbJob.JobID,
		s.stringInterner.Intern(dbJob.JobSet),
		s.stringInterner.Intern(dbJob.Queue),
		uint32(dbJob.Priority),
		schedulingInfo,
		dbJob.CancelRequested,
		dbJob.CancelByJobsetRequested,
		dbJob.Cancelled,
		dbJob.Submitted,
	), nil
}

// createSchedulerRun creates a new scheduler job run from a database job run
func (s *Scheduler) createSchedulerRun(dbRun *database.Run) *jobdb.JobRun {
	return jobdb.CreateRun(
		dbRun.RunID,
		dbRun.Created,
		s.stringInterner.Intern(dbRun.Executor),
		s.stringInterner.Intern(dbRun.Node),
		dbRun.Running,
		dbRun.Succeeded,
		dbRun.Failed,
		dbRun.Cancelled,
		dbRun.Returned)
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
	return run
}

// updateSchedulerJob updates the scheduler job  (in-place) to match the database job
func updateSchedulerJob(job *jobdb.Job, dbJob *database.Job) *jobdb.Job {
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
	return job
}
