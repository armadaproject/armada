package eventscheduler

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// While not the leader, try to become the leader.
// For now, let's just have it continually populate it's internal state.
type Scheduler struct {
	Jobs        map[uuid.UUID]*Job
	JobsSerial  int64
	Runs        map[uuid.UUID]*Run
	RunsSerial  int64
	Nodes       map[string]*Nodeinfo
	NodesSerial int64
	Db          *pgxpool.Pool
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

func NewScheduler(db *pgxpool.Pool) *Scheduler {
	return &Scheduler{
		Jobs:        make(map[uuid.UUID]*Job),
		JobsSerial:  -1,
		Runs:        make(map[uuid.UUID]*Run),
		RunsSerial:  -1,
		Nodes:       make(map[string]*Nodeinfo),
		NodesSerial: -1,
		Db:          db,
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

	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
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
	}
	return nil
}

func (srv *Scheduler) updateJobs(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)
	queries := New(srv.Db)
	jobs, err := queries.SelectNewJobs(ctx, srv.JobsSerial)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("got updated jobs: %+v", jobs)
	for _, job := range jobs {
		job := job
		srv.Jobs[job.JobID] = &job
		if job.Serial > srv.JobsSerial {
			srv.JobsSerial = job.Serial
		}
	}
	return nil
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
		if run.Serial > srv.JobsSerial {
			srv.JobsSerial = run.Serial
		}
	}
	return nil
}
