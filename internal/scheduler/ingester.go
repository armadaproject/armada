package scheduler

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Service that updates the scheduler database.
//
// At a high level, the ingester:
// 1. Reads messages from Pulsar, which are used to create DbOperations.
// 2. Db ops are collected for up to some amount of time.
// 3. Db ops are applied to postgres in batch.
// 4. The Pulsar messages read to produce the ops are acked.
type Ingester struct {
	// Used to setup a Pulsar consumer.
	PulsarClient    pulsar.Client
	ConsumerOptions pulsar.ConsumerOptions
	// Pulsar consumer on which to receive messages.
	// Created by the service.
	consumer pulsar.Consumer
	// Connection to the postgres database.
	Db *pgxpool.Pool
	// Write to postgres at least this often (assuming there are records to write).
	MaxWriteInterval time.Duration
	// Max number of DbOperation to batch.
	MaxDbOps int
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

// Run the ingester until experiencing an unrecoverable error.
func (srv *Ingester) Run(ctx context.Context) error {
	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerIngester")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerIngester")
	}
	log.Info("service started")
	defer log.Info("service stopped")

	// Scheduler ingester.
	consumer, err := srv.PulsarClient.Subscribe(srv.ConsumerOptions)
	if err != nil {
		return errors.WithStack(err)
	}
	defer consumer.Close()
	srv.consumer = consumer
	g, ctx := errgroup.WithContext(ctx)
	ctx = ctxlogrus.ToContext(ctx, log)

	// Receive Pulsar messages.
	pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.consumer)
	g.Go(func() error { return pulsarToChannel.Run(ctx) })

	// Unmarshal into event sequences.
	sequenceFromMessage := eventutil.NewSequenceFromMessage(pulsarToChannel.C)
	g.Go(func() error { return sequenceFromMessage.Run(ctx) })

	// Discard submit job messages not intended for this scheduler.
	submitJobFilter := eventutil.NewEventFilter(sequenceFromMessage.Out, func(event *armadaevents.EventSequence_Event) bool {
		// Discard if a SubmitJob event that doesn't target this scheduler.
		if e := event.GetSubmitJob(); e != nil && e.Scheduler != "pulsar" {
			return false
		}
		return true
	})
	g.Go(func() error { return submitJobFilter.Run(ctx) })

	// Compact sequences to produce fewer sequences with more events,
	// where doing so does not change inter-job set ordering.
	// TODO: Prob. not necessary, since we anyway compact database operations. // Albin
	sequenceCompacter := eventutil.NewSequenceCompacter(submitJobFilter.Out)
	g.Go(func() error { return sequenceCompacter.Run(ctx) })

	// Convert events into optimised batches of database operations.
	dbOpsBatcher := NewDbOperationsBatcher(sequenceCompacter.Out)
	g.Go(func() error { return dbOpsBatcher.Run(ctx) })

	// Convert events into optimised batches of databasne operations.
	dbOpsWriter := &DbOpsWriter{
		In:       dbOpsBatcher.Out,
		Db:       srv.Db,
		Consumer: srv.consumer,
	}
	g.Go(func() error { return dbOpsWriter.Run(ctx) })

	// Run pipeline until any error.
	return g.Wait() // TODO: Detect recoverable errors.
}
