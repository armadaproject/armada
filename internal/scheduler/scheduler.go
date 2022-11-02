package scheduler

import (
	"context"
	"math/rand"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Scheduler implements a trivial scheduling algorithm.
// It's here just to test that the scheduling subsystem as a whole is working.
type Scheduler struct {
	Producer pulsar.Producer
	Db       *pgxpool.Pool
	// Map from job id to job struct.
	// Contains all jobs the scheduler is aware of that have not terminated,
	// i.e., succeed, failed, or been cancelled.
	// Jobs are added when read from postgres.
	// Jobs are removed when they terminate
	ActiveJobs map[uuid.UUID]*Job
	// Map from job id to a collection of all runs associated with that job.
	RunsByJobId map[uuid.UUID]*JobRuns
	// The queue consists of all active jobs that don't have at least one active run associated with it.
	// Ids of all jobs that don't have at least one active
	// Ids of jobs that have not yet been scheduled.
	QueuedJobIds []uuid.UUID
	// List of worker nodes available across all clusters.
	Nodes map[string]*Nodeinfo
	// Map from executor name to the last time we heard from that executor.
	Executors map[string]time.Time
	// Amount of time after which an executor is assumed to be unavailable
	// if no updates have been received.
	ExecutorAliveDuration time.Duration
	// Each write into postgres is marked with an increasing serial number.
	// For each table, we store the largest number seen so far.
	// When reading from postgres, we select all rows with a serial number larger than what we've seen so far.
	//
	// The first record written to postgres for each table will have serial 1,
	// so these should be initialised to 0.
	JobsSerial               int64
	RunsSerial               int64
	JobRunsAssignmentsSerial int64
	JobErrorsSerial          int64
	JobRunErrorsSerial       int64
	NodesSerial              int64
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

// JobRuns is a collection of all runs associated with a particular job.
type JobRuns struct {
	// Any runs associated with this job that have not terminated.
	// Map from run id to run.
	ActiveRuns map[uuid.UUID]*Run
	// Any runs associated with this job for which the ingester has received
	// a terminal event (i.e., succeeded, failed, or cancelled).
	// Map from run id to run.
	InactiveRuns map[uuid.UUID]*Run
}

func NewJobRuns() *JobRuns {
	return &JobRuns{
		ActiveRuns:   make(map[uuid.UUID]*Run),
		InactiveRuns: make(map[uuid.UUID]*Run),
	}
}

func NewScheduler(producer pulsar.Producer, db *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Producer:              producer,
		Db:                    db,
		ActiveJobs:            make(map[uuid.UUID]*Job),
		RunsByJobId:           make(map[uuid.UUID]*JobRuns),
		Nodes:                 make(map[string]*Nodeinfo),
		Executors:             make(map[string]time.Time),
		ExecutorAliveDuration: 5 * time.Minute,
	}
}

func (srv *Scheduler) Run(ctx context.Context) error {
	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerIngester")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerIngester")
	}
	log.Info("service started")

	refreshTicker := time.NewTicker(10 * time.Second)
	scheduleTicker := time.NewTicker(11 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-refreshTicker.C:
			g, groupCtx := errgroup.WithContext(ctx)
			entry := log.WithFields(logrus.Fields{
				"JobsSerial":  srv.JobsSerial,
				"RunsSerial":  srv.RunsSerial,
				"NodesSerial": srv.NodesSerial,
			})
			ctxWithLogger := ctxlogrus.ToContext(groupCtx, entry)
			g.Go(func() error { return srv.updateJobsRuns(ctxWithLogger) })
			g.Go(func() error { return srv.updateNodes(ctxWithLogger) })
			err := g.Wait()
			if err != nil {
				logging.WithStacktrace(log, err).Info("failed to read from postgres")
			}
		case <-scheduleTicker.C:
			err := srv.schedule(ctx)
			if err != nil {
				logging.WithStacktrace(log, err).Info("failed to schedule jobs")
			}
		}
	}
}

func (srv *Scheduler) updateNodes(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)
	nodes, err := queries.SelectNewNodeInfo(ctx, srv.NodesSerial)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("got updated nodes: %+v", nodes)
	for _, node := range nodes {
		node := node
		srv.Nodes[node.NodeName] = &node
		if node.Serial > srv.NodesSerial {
			srv.NodesSerial = node.Serial
		}
		srv.Executors[node.Executor] = node.LastModified
	}
	return nil
}

func (srv *Scheduler) updateJobsRuns(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)

	// New jobs.
	// TODO: We shouldn't load all columns.
	// TODO: The first time this runs, we should get only active jobs. After that we should get all jobs.
	// jobs, err := queries.SelectNewActiveJobs(ctx, srv.JobsSerial)
	jobs, err := queries.SelectNewJobs(ctx, srv.JobsSerial)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("got updated jobs: %+v", jobs)
	for _, job := range jobs {
		job := job

		// Keep the serial number updated.
		if job.Serial > srv.JobsSerial {
			srv.JobsSerial = job.Serial
		}

		// If the job is inactive, remove it from the set of active jobs
		// and remove all runs associated with the job.
		if job.Cancelled || job.Succeeded || job.Failed {
			delete(srv.ActiveJobs, job.JobID)
			delete(srv.RunsByJobId, job.JobID)
			continue
		}

		// If we've never seen this job before, it should be added to the queue.
		if _, ok := srv.ActiveJobs[job.JobID]; !ok {
			srv.QueuedJobIds = append(srv.QueuedJobIds, job.JobID)
		}

		// Add the job to the set of active jobs.
		srv.ActiveJobs[job.JobID] = &job
	}

	// New runs for active jobs.
	i := 0
	jobIds := make([]uuid.UUID, len(srv.ActiveJobs))
	for jobId := range srv.ActiveJobs {
		jobIds[i] = jobId
		i++
	}
	runs, err := queries.SelectNewRunsForJobs(ctx, SelectNewRunsForJobsParams{
		JobIds: jobIds,
		Serial: srv.RunsSerial,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	for _, run := range runs {
		run := run

		// Keep the serial number updated.
		if run.Serial > srv.RunsSerial {
			srv.RunsSerial = run.Serial
		}

		jobRuns, ok := srv.RunsByJobId[run.JobID]
		if !ok {
			jobRuns = NewJobRuns()
			srv.RunsByJobId[run.JobID] = jobRuns
		}

		// Move runs that terminate from the set of active runs to the set of inactive runs,
		// and consider if the job should be rescheduled.
		if run.Cancelled || run.Succeeded || run.Failed {
			delete(jobRuns.ActiveRuns, run.RunID)
			jobRuns.InactiveRuns[run.RunID] = &run
			if srv.shouldReScheduleJob(jobRuns) {
				srv.enqueueJob(run.JobID)
			}
			continue
		}
	}

	return nil
}

func (srv *Scheduler) shouldReScheduleJob(jobRuns *JobRuns) bool {
	// TODO: Look into any previous runs to determine if we should try running the job again.
	if len(jobRuns.ActiveRuns) == 0 {
		return false
	}
	return false
}

func (srv *Scheduler) enqueueJob(jobId uuid.UUID) {
	srv.QueuedJobIds = append(srv.QueuedJobIds, jobId)
}

func (srv *Scheduler) schedule(ctx context.Context) error {
	executors := srv.getActiveExecutors()
	if len(executors) == 0 {
		if len(srv.Executors) == 0 {
			return errors.New("no executors available")
		} else {
			return errors.Errorf("all executors inactive: %+v", srv.Executors)
		}
	}

	// Assign jobs in the queue to executors in a round-robin fashion.
	// We create one sequence per lease here.
	// Before sending to Pulsar, these are reduced to the minimal number possible.
	i := rand.Intn(len(executors))
	sequences := make([]*armadaevents.EventSequence, 0)
	for _, jobId := range srv.QueuedJobIds {
		job, ok := srv.ActiveJobs[jobId]
		if !ok {
			continue
		}
		sequences = append(sequences, &armadaevents.EventSequence{
			Queue:      job.Queue,
			JobSetName: job.JobSet,
			// UserId and Groups can be safely omitted here.
			// Since they're sent to the executor together with the job spec.
			// UserId: job.UserID,
			// Groups: job.Groups,
			Events: []*armadaevents.EventSequence_Event{
				{
					Event: &armadaevents.EventSequence_Event_JobRunLeased{
						JobRunLeased: &armadaevents.JobRunLeased{
							RunId:      armadaevents.ProtoUuidFromUuid(uuid.New()),
							JobId:      armadaevents.ProtoUuidFromUuid(job.JobID),
							ExecutorId: executors[i],
						},
					},
				},
			},
		})
		i = (i + 1) % len(executors)
	}

	// Since we always create leases for all jobs, empty the queue.
	srv.QueuedJobIds = make([]uuid.UUID, 0)

	// Reduce the number of sequences to the minimal number possible,
	// and publish to Pulsar.
	return pulsarutils.CompactAndPublishSequences(ctx, sequences, srv.Producer, 4194304) // 4 MB
}

func (srv *Scheduler) getActiveExecutors() []string {
	activeExecutors := make([]string, 0)
	for executorName, lastSeen := range srv.Executors {
		if time.Since(lastSeen) < srv.ExecutorAliveDuration {
			activeExecutors = append(activeExecutors, executorName)
		}
	}
	return activeExecutors
}
