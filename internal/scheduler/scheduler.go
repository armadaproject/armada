package scheduler

import (
	"context"
	"github.com/hashicorp/go-memdb"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/G-Research/armada/internal/scheduler/database"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/G-Research/armada/pkg/armadaevents"
)

type Scheduler struct {
	// Provides job updates from Postgres
	jobRepository database.JobRepository
	// Used to determine whether a cluster is active
	clusterRepository database.ClusterRepository
	// Responsible for assigning jobs to nodes
	schedulingAlgo SchedulingAlgo
	// Tells us if we are leader. Only the leader may schedule jobs
	leaderController LeaderController
	// Responsible for publishing messaged to Pulsar.  Only the leader publishes
	publisher Publisher
	// Minimum duration between scheduling cycles.
	cyclePeriod time.Duration
	// Maximum number of times a lease can be returned before the job is considered failed
	maxLeaseReturns uint
	// ClusterTimeout
	clusterTimeout time.Duration
	// Used for all timing decisions (sleep etc). Injected here so that we can mock out for testing
	clock clock.Clock
	// Stores active jobs (i.e queued/running) and provides fast in-memory lookups on them
	jobDb *JobDb
	// Highest offset we've read from Postgres on the Jobs table.
	jobsSerial int64
	// Highest offset we've read from Postgres on the Job Runs table.
	runsSerial int64
}

func NewScheduler(
	jobRepository database.JobRepository,
	clusterRepository database.ClusterRepository,
	schedulingAlgo SchedulingAlgo,
	leaderController LeaderController,
	publisher Publisher,
	cyclePeriod time.Duration,
	clusterTimeout time.Duration,
	maxLeaseReturns uint) (*Scheduler, error) {
	jobDb, err := NewJobDb()
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		jobRepository:     jobRepository,
		clusterRepository: clusterRepository,
		schedulingAlgo:    schedulingAlgo,
		leaderController:  leaderController,
		publisher:         publisher,
		jobDb:             jobDb,
		clock:             clock.RealClock{},
		cyclePeriod:       cyclePeriod,
		clusterTimeout:    clusterTimeout,
		maxLeaseReturns:   maxLeaseReturns,
		jobsSerial:        -1,
		runsSerial:        -1,
	}, nil
}

func (s *Scheduler) Run(ctx context.Context) error {

	//  Do initial population of Job Db
	err := s.initialise(ctx)
	if err != nil {
		return err
	}

	ticker := s.clock.NewTicker(s.cyclePeriod)

	var prevLeaderToken = LeaderToken{leader: false, id: uuid.New()}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			start := s.clock.Now()
			leaderToken := s.leaderController.GetToken()
			if leaderToken.leader && leaderToken != prevLeaderToken {
				err := s.becomeMaster(ctx, 1*time.Minute)
				if err != nil {
					log.WithError(err).Error("Could not become master")
					leaderToken = NewFollowerToken()
				}
			}
			err := s.doCycle(ctx, false, leaderToken)
			if err != nil {
				log.WithError(err).Error("Error in scheduling cycle")
				leaderToken = NewFollowerToken()
			}
			taken := s.clock.Now().Sub(start)
			log.Infof("Completed scheduling cycle in %s", taken)
			prevLeaderToken = leaderToken
		}
	}
}

func (s *Scheduler) doCycle(ctx context.Context, updateAll bool, leaderToken LeaderToken) error {

	// Update job state
	updatedJobs, err := s.syncState(ctx)
	if err != nil {
		return err
	}

	// Am I leader? If not then we're done
	if !s.leaderController.ValidateToken(leaderToken) {
		return nil
	}

	// start a transaction- we'll roll this back if we don't publish
	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	// If we've been asked to generate messages for all jobs do so, else generate messages only for jobs updated this cycle
	if updateAll {
		updatedJobs, err = s.jobDb.GetAll(txn)
		if err != nil {
			return err
		}
	}

	// Generate any events that came out of synchronising the db state
	events, err := s.generateUpdateMessages(ctx, updatedJobs)
	if err != nil {
		return err
	}

	// Remove any jobs that have moved into a terminal state as a result of the above actions
	err = s.removeTerminalJobs(txn, updatedJobs)
	if err != nil {
		return err
	}

	// Expire any jobs running on clusters that haven't heartbeated within our time limit
	expirationEvents, err := s.expireJobsIfNecessary(txn)
	if err != nil {
		return err
	}
	events = append(events, expirationEvents...)

	// Schedule Jobs
	scheduledJobs, err := s.schedulingAlgo.Schedule(txn, s.jobDb)
	if err != nil {
		return err
	}
	scheduledJobEvents, err := s.generateLeaseMessages(scheduledJobs)
	events = append(events, scheduledJobEvents...)

	err = s.publisher.PublishMessages(ctx, events, leaderToken)
	if err != nil {
		return err
	}
	txn.Commit()
	return nil
}

func (s *Scheduler) syncState(ctx context.Context) ([]*SchedulerJob, error) {

	updatedJobs, updatedRuns, err := s.jobRepository.FetchJobUpdates(ctx, s.jobsSerial, s.runsSerial)
	if err != nil {
		return nil, err
	}
	log.Infof("Received %d updated jobs and %d updated job runs", len(updatedJobs), len(updatedRuns))

	txn := s.jobDb.WriteTxn()
	defer txn.Abort()

	jobsToDelete := make([]string, 0, len(updatedJobs))
	jobsToUpdateById := make(map[string]*SchedulerJob, len(updatedJobs))
	for _, dbJob := range updatedJobs {
		jobId := dbJob.JobIdString()
		// Scheduler has sent a terminal message therefore we can safely remove the job
		if dbJob.Terminal() {
			jobsToDelete = append(jobsToDelete, jobId)
		}

		// Try and retrieve the job from the jobDb.  If it doesn't exist then create it.
		job, err := s.jobDb.GetById(txn, jobId)
		if err != nil {
			return nil, errors.Wrapf(err, "error retrieving job %s from jobDb ", jobId)
		}
		job = job.DeepCopy()
		if job == nil {
			job, err = createSchedulerJob(&dbJob)
			if err != nil {
				return nil, err
			}
		} else {
			// make the scheduler job look like the db job
			updateSchedulerJob(job, &dbJob)
		}
		jobsToUpdateById[jobId] = job
	}

	for _, dbRun := range updatedRuns {
		jobId := dbRun.JobID

		// Retrieve the job, look first in the list of updates, then in the jobDb
		job, present := jobsToUpdateById[jobId]
		if !present {
			job, err = s.jobDb.GetById(txn, jobId)
			if err != nil {
				return nil, errors.Wrapf(err, "error retrieving job %s from jobDb ", jobId)
			}
			job = job.DeepCopy()
			jobsToUpdateById[jobId] = job
		}

		// If the job is nil at this point then it cannot be active.
		// In this case we can ignore the run
		if job == nil {
			log.Debugf("Job %s is not an active job. Ignoring update for run %s", jobId, dbRun.RunID)
			continue
		}

		var returnProcessed = false
		run, ok := job.RunById(dbRun.RunID)
		if !ok {
			run = createSchedulerRun(&dbRun)
			job.Runs = append(job.Runs, run)
		} else {
			returnProcessed = run.Returned
			// make the scheduler job look like the db job
			updateSchedulerRun(run, &dbRun)
		}

		// work out if the job needs to be re-queued.  This is a bit awkward as the old scheduler
		// didn't send an explicit queued message here which means we have to infer it. For now we
		// do the same, but eventually we should send an actual queued message and this bit of code can disappear
		if !returnProcessed && run.Returned && job.NumReturned() <= s.maxLeaseReturns {
			job.Leased = false
			run.Failed = false // unset failed here so that we don't generate a job failed message later
		}

	}
	jobsToUpdate := maps.Values(jobsToUpdateById)
	err = s.jobDb.BatchDeleteById(txn, jobsToDelete)
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

func (s *Scheduler) generateLeaseMessages(scheduledJobs []*SchedulerJob) ([]*armadaevents.EventSequence, error) {
	events := make([]*armadaevents.EventSequence, len(scheduledJobs))
	for i, job := range scheduledJobs {
		jobId, err := armadaevents.ProtoUuidFromUlidString(job.JobId)
		if err != nil {
			return nil, err
		}
		es := &armadaevents.EventSequence{
			Queue:      job.Queue,
			JobSetName: job.Jobset,
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: s.now(),
					Event: &armadaevents.EventSequence_Event_JobRunLeased{
						JobRunLeased: &armadaevents.JobRunLeased{
							RunId:      armadaevents.ProtoUuidFromUuid(job.CurrentRun().RunID),
							JobId:      jobId,
							ExecutorId: job.Executor,
						},
					},
				},
			},
		}
		events[i] = es
	}
	return events, nil
}

func (s *Scheduler) removeTerminalJobs(txn *memdb.Txn, updatedJobs []*SchedulerJob) error {
	idsToDelete := make([]string, 0)
	for _, job := range updatedJobs {
		if job.Succeeded || job.Cancelled || job.Failed {
			idsToDelete = append(idsToDelete, job.JobId)
		}
	}
	return s.jobDb.BatchDeleteById(txn, idsToDelete)
}

func (s *Scheduler) generateUpdateMessages(ctx context.Context, updatedJobs []*SchedulerJob) ([]*armadaevents.EventSequence, error) {

	failedRunIds := make([]uuid.UUID, 0, len(updatedJobs))
	for _, job := range updatedJobs {
		run := job.CurrentRun()
		if run != nil && run.Failed {
			failedRunIds = append(failedRunIds, run.RunID)
		}
	}
	jobRunErrors, err := s.jobRepository.FetchJobRunErrors(ctx, failedRunIds)
	if err != nil {
		return nil, err
	}

	// Generate any events that came out of synchronising the db state
	var events []*armadaevents.EventSequence
	for _, job := range updatedJobs {
		jobEvents, err := s.updateMessageFromJob(job, jobRunErrors)
		if err != nil {
			return nil, err
		}
		if jobEvents != nil {
			events = append(events, jobEvents)
		}
	}
	return events, nil
}

func (s *Scheduler) expireJobsIfNecessary(txn *memdb.Txn) ([]*armadaevents.EventSequence, error) {
	heartbeatTimes, err := s.clusterRepository.GetLastUpdateTimes()
	if err != nil {
		return nil, err
	}
	missingClusters := make(map[string]bool, 0)
	cutOff := s.clock.Now().Add(-s.clusterTimeout)
	for cluster, heartbeat := range heartbeatTimes {
		if heartbeat.Before(cutOff) {
			log.Warnf("Cluster %s has not reported a hearbeart since %v. Will expire all jobs running on this cluster", cluster, heartbeat)
			missingClusters[cluster] = true
		}
	}

	// All clusters have had a heartbeat recently.  No need to expire any jobs
	if len(missingClusters) == 0 {
		return nil, nil
	}

	events := make([]*armadaevents.EventSequence, 0)

	//TODO: this is inefficient.  We should create a iterator of the jobs running on the affected clusters
	jobs, err := s.jobDb.GetAll(txn)
	if err != nil {
		return nil, err
	}

	jobsToDelete := make([]string, 0)
	for _, job := range jobs {
		run := job.CurrentRun()
		if job.Leased && run != nil && missingClusters[run.Executor] {
			log.Warnf("Cancelling job %s as it is running on lost cluster %s", job.JobId, run.Executor)
			jobId, err := armadaevents.ProtoUuidFromUlidString(job.JobId)
			if err != nil {
				return nil, err
			}

			leaseExpiredErrror := &armadaevents.Error{
				Terminal: true,
				Reason: &armadaevents.Error_LeaseExpired{
					LeaseExpired: &armadaevents.LeaseExpired{},
				},
			}
			es := &armadaevents.EventSequence{
				Queue:      job.Queue,
				JobSetName: job.Jobset,
				Events: []*armadaevents.EventSequence_Event{
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobRunErrors{
							JobRunErrors: &armadaevents.JobRunErrors{
								RunId:  armadaevents.ProtoUuidFromUuid(run.RunID),
								JobId:  jobId,
								Errors: []*armadaevents.Error{leaseExpiredErrror},
							},
						},
					},
					{
						Created: s.now(),
						Event: &armadaevents.EventSequence_Event_JobErrors{
							JobErrors: &armadaevents.JobErrors{
								JobId:  jobId,
								Errors: []*armadaevents.Error{leaseExpiredErrror},
							},
						},
					},
				},
			}
			events = append(events, es)
			jobsToDelete = append(jobsToDelete, job.JobId)
		}
	}
	s.jobDb.BatchDeleteById(txn, jobsToDelete)
	return events, nil
}

func (s *Scheduler) updateMessageFromJob(job *SchedulerJob, jobRunErrors map[uuid.UUID]*armadaevents.JobRunErrors) (*armadaevents.EventSequence, error) {

	var events []*armadaevents.EventSequence_Event

	// Is the job already in a terminal state?  If so then don't send any more messages
	if job.Cancelled || job.Succeeded || job.Failed {
		return nil, nil
	}

	jobId, err := armadaevents.ProtoUuidFromUlidString(job.JobId)
	if err != nil {
		return nil, err
	}

	// Has the job been requested cancelled.  If so, cancel the job
	if job.CancelRequested {
		job.Cancelled = true
		cancel :=
			&armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_CancelledJob{
					CancelledJob: &armadaevents.CancelledJob{JobId: jobId},
				},
			}
		events = append(events, cancel)
	} else if len(job.Runs) > 0 {
		lastRun := job.CurrentRun()
		// Terminal states. Can only have one of these
		if lastRun.Succeeded {
			job.Succeeded = true
			jobSucceeded := &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobSucceeded{
					JobSucceeded: &armadaevents.JobSucceeded{
						JobId: jobId,
					},
				},
			}
			events = append(events, jobSucceeded)
		} else if lastRun.Failed || lastRun.Expired {
			job.Failed = true
			errors := jobRunErrors[lastRun.RunID]
			jobErrors := &armadaevents.EventSequence_Event{
				Created: s.now(),
				Event: &armadaevents.EventSequence_Event_JobErrors{
					JobErrors: &armadaevents.JobErrors{
						JobId:  jobId,
						Errors: errors.GetErrors(),
					},
				},
			}
			events = append(events, jobErrors)
		}
	}

	if len(events) > 0 {
		return &armadaevents.EventSequence{
			Queue:      job.Queue,
			JobSetName: job.Jobset,
			Events:     events,
		}, nil
	}

	return nil, nil
}

func (s *Scheduler) now() *time.Time {
	now := s.clock.Now()
	return &now
}

func createSchedulerJob(dbJob *database.Job) (*SchedulerJob, error) {
	schedulingInfo := &schedulerobjects.JobSchedulingInfo{}
	err := proto.Unmarshal(dbJob.SchedulingInfo, schedulingInfo)
	if err != nil {
		return nil, errors.Wrapf(
			errors.WithStack(err), "error unmarshalling scheduling info for job %s", dbJob.JobIdString())
	}
	return &SchedulerJob{
		JobId:             dbJob.JobIdString(),
		Jobset:            dbJob.JobSet,
		Queue:             dbJob.Queue,
		Leased:            false,
		Priority:          uint32(dbJob.Priority),
		jobSchedulingInfo: schedulingInfo,
		CancelRequested:   dbJob.CancelRequested,
		Cancelled:         dbJob.Cancelled,
		Timestamp:         dbJob.Submitted.UnixMicro(),
	}, nil
}

func (s *Scheduler) initialise(ctx context.Context) error {
	for {
		_, err := s.syncState(ctx)
		if err != nil {
			log.WithError(err).Error("Error initialising")
		} else {
			break
		}
	}
	return nil
}

func (s *Scheduler) becomeMaster(ctx context.Context, pollInterval time.Duration) error {
	var groupId uuid.UUID
	var numSent uint32
	var err error
	for {
		numSent, err = s.publisher.PublishMarkers(ctx, groupId)
		if err != nil {
			log.WithError(err).Error("Error counting received partitions")
		} else {
			break
		}
	}

	for {
		numRecevied, err := s.jobRepository.CountReceivedPartitions(ctx, groupId)
		if err != nil {
			log.WithError(err).Error("Error counting received partitions")
		}
		if numSent == numRecevied {
			return nil
		}
		s.clock.Sleep(pollInterval)
	}
}

func createSchedulerRun(dbRun *database.Run) *JobRun {
	return &JobRun{
		RunID:     dbRun.RunID,
		Executor:  dbRun.Executor,
		Running:   dbRun.Running,
		Succeeded: dbRun.Succeeded,
		Failed:    dbRun.Failed,
		Cancelled: dbRun.Cancelled,
		Returned:  dbRun.Returned,
	}
}

func updateSchedulerRun(run *JobRun, dbRun *database.Run) {
	run.Succeeded = dbRun.Succeeded
	run.Failed = dbRun.Failed
	run.Cancelled = dbRun.Cancelled
	run.Returned = dbRun.Returned
}

func updateSchedulerJob(job *SchedulerJob, dbJob *database.Job) {
	job.CancelRequested = dbJob.CancelRequested
	job.Succeeded = dbJob.Succeeded
	job.Cancelled = dbJob.Cancelled
	job.Failed = dbJob.Failed
}
