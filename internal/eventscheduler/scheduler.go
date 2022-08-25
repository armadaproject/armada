package eventscheduler

import (
	"context"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// While not the leader, try to become the leader.
// For now, let's just have it continually populate it's internal state.
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
	ActiveRuns     []*Run
	TerminatedRuns []*Run
}

func NewScheduler(db *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Jobs:                  make(map[uuid.UUID]*Job),
		Runs:                  make(map[uuid.UUID]*Run),
		Nodes:                 make(map[string]*Nodeinfo),
		Db:                    db,
		Executors:             make(map[string]time.Time),
		ExecutorAliveDuration: time.Minute,
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
	scheduleTicker := time.NewTicker(1 * time.Second)
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
			g.Go(func() error { return srv.updateJobs(ctxWithLogger) })
			g.Go(func() error { return srv.updateRuns(ctxWithLogger) })
			g.Go(func() error { return srv.updateNodes(ctxWithLogger) })
			err := g.Wait()
			if err != nil {
				return err
			}
		case <-scheduleTicker.C:

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
	jobs, err := queries.SelectNewActiveJobs(ctx, srv.JobsSerial)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("got updated jobs: %+v", jobs)
	for _, job := range jobs {
		job := job

		// If the job is inactive, remove it from the set of active jobs
		// and remove all runs associated with the job.
		if job.Cancelled || job.Succeeded || job.Failed {
			delete(srv.ActiveJobs, job.JobID)
			delete(srv.RunsByJobId, job.JobID)
		}

		// If we've never seen this job before, it should be added to the queue.
		if _, ok := srv.ActiveJobs[job.JobID]; !ok {
			srv.QueuedJobIds = append(srv.QueuedJobIds, job.JobID)
		}

		// Add the job to the set of active jobs.
		srv.ActiveJobs[job.JobID] = &job

		// Keep the serial number updated.
		if job.Serial > srv.JobsSerial {
			srv.JobsSerial = job.Serial
		}
	}

	// New runs for all active jobs.
	i := 0
	jobIds := make([]uuid.UUID, len(srv.ActiveJobs))
	for jobId, _ := range srv.ActiveJobs {
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

	//

	return nil
}

// Whenever we see a terminal run failure, we need to decide if it should be enqueued again.
// At that point, we should also give back some resource usage to the owner of that job.

// updateJob updates the in-memory representation of a job.
// This function is called when an existing job is changed.
func (srv *Scheduler) updateJob(ctx context.Context, job *Job) error {
	// TODO: Update job priority.
	// If the job is cancelled, we need to cancel any outstanding runs.
	// Actually, a cancelled run fails lease renewal.
}

func (srv *Scheduler) updateRuns(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)
	runs, err := queries.SelectNewRuns(ctx, srv.RunsSerial)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("got updated runs: %+v", runs)
	for _, run := range runs {
		run := run
		srv.Runs[run.RunID] = &run
		if run.Serial > srv.RunsSerial {
			srv.RunsSerial = run.Serial
		}
	}
	return nil
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
	for _, job := range srv.JobQueue {
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
