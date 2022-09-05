package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// Service responsible for writing records derived from pulsar messages into postgres.
//
// At a high level, the ingester:
// 1. Reads messages from pulsar, which are used to create records.
// 2. Records are collected for up to some amount of time or records.
// 3. Records are batch-upserted into postgres.
// 4. The pulsar messages read to produce the batch are acked.
//
// The ingester handles the following messages:
//   - SubmitJob and JobRunLeased.
//     Creates new records in the jobs and runs table, respectively.
//   - ReprioritiseJob and ReprioritiseJobSet.
//     Updates the priority column for these jobs in-place. Does not mutate the job spec.
//   - CancelJob, CancelJobSet, JobSucceeded, and JobErrors.
//     Updates the state column for these jobs in-place.
//     Non-terminal JobErrors messages are ignored.
//   - JobRunAssigned.
//     Updates the runs table in-place with info of where the job is running.
//   - JobRunRunning, JobRunSucceeded, and JobRunErrors.
//     Updates the state column for these runs in-place.
//     Non-terminal JobRunErrors messages are ignored.
//
// Each ingester instance can only write into a single postgres table.
// I.e., to populate multiple tables with different record types (e.g., jobs and leases),
// a separate ingester instance is required for each record type.
type Ingester struct {
	// Used to setup a Pulsar consumer.
	PulsarClient    pulsar.Client
	ConsumerOptions pulsar.ConsumerOptions
	// Write to postgres at least this often (assuming there are records to write).
	MaxWriteInterval time.Duration
	// Write current batch to postgres if at least this many records have been written to it.
	MaxWriteRecords int
	// Connection to the postgres database used for persistence.
	Db *pgxpool.Pool
	// For each partition, store the id of the most recent message.
	// Used to detect unexpected seeks.
	lastMessageIdByPartition map[int32]pulsar.MessageID
	// Pulsar consumer on which to receive messages.
	consumer pulsar.Consumer
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

	// Create a processing pipeline
	// receive from pulsar -> batch messages -> write to postgres and ack messages.
	for {

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

		// Batch messages for writing into postgres.
		// Batches are forwarded on batchChannel.
		batchChannel := make(chan *Batch)
		g.Go(func() error { return srv.ProcessMessages(ctx, pulsarToChannel.C, batchChannel) })

		// Write batches to postgres and, for each batch, ack all pulsar messages the batch was made up of.
		g.Go(func() error { return srv.ProcessBatches(ctx, batchChannel) })

		// Run pipeline until any error.
		err = g.Wait()
		// TODO: Detect recoverable errors.
		if err != nil {
			// Unrecoverable error; exit.
			return err
		}
	}
}

func (srv *Ingester) ProcessMessages(ctx context.Context, messageChannel <-chan pulsar.Message, batchChannel chan<- *Batch) error {
	log := ctxlogrus.Extract(ctx)

	// In-progress batches.
	batches := make([]*Batch, 0)

	// Ticker to trigger periodically writing to postgres.
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	// Run until ctx is canceled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-messageChannel:

			// Incoming gRPC requests are annotated with a unique id,
			// which is included with the corresponding Pulsar message.
			requestId := pulsarrequestid.FromMessageOrMissing(msg)

			// Put the requestId into a message-specific context and logger,
			// which are passed on to sub-functions.
			messageCtx, ok := requestid.AddToIncomingContext(ctx, requestId)
			if !ok {
				messageCtx = ctx
			}
			messageLogger := log.WithFields(logrus.Fields{"messageId": msg.ID(), requestid.MetadataKey: requestId})
			ctxWithLogger := ctxlogrus.ToContext(messageCtx, messageLogger)

			// Any consumer can seek on any topic partition.
			// An unexpected seek may cause messages to be delivered out of order,
			// i.e., a received messages may not be newer than the previous message.
			//
			// We compare the id of each received message with that of the previous message
			// on the same partition to detect such out-of-order messages.
			//
			// Note that, while possible, this should be a rare occurrence.
			//
			// TODO: We should check for this and restart the consumer if it happens.
			partitionIdx := msg.ID().PartitionIdx()
			if srv.lastMessageIdByPartition == nil {
				srv.lastMessageIdByPartition = make(map[int32]pulsar.MessageID)
			}
			if lastMessageId, ok := srv.lastMessageIdByPartition[partitionIdx]; ok {
				msgIsOufOfOrder, err := pulsarutils.FromMessageId(lastMessageId).GreaterEqual(msg.ID())
				if err != nil {
					return err
				}
				if msgIsOufOfOrder {
					// pulsarutils.PulsarMessageId prints nicely, so we convert to those.
					return fmt.Errorf(
						"unexpected seek detected: received messages out of order for topic %s: id of received message is %s, but the previous message has id %s",
						msg.Topic(), pulsarutils.FromMessageId(msg.ID()), pulsarutils.FromMessageId(lastMessageId),
					)
				}
			}
			srv.lastMessageIdByPartition[partitionIdx] = msg.ID()

			var err error
			batches, err = srv.ProcessMessage(ctxWithLogger, msg, batches)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
			}

			// If we have more than 1 batch, apply all but the last immedtately.
			batchesToApply := make([]*Batch, 0)
			if len(batches) > 1 {
				batchesToApply = append(batchesToApply, batches[:len(batches)-1]...)
				batches = []*Batch{batches[len(batches)-1]}
			}

			// If the most recent batch is large enough,
			// apply it immedtately to avoid batches becoming too large.
			if len(batches) == 1 {
				batch := batches[len(batches)-1]
				if srv.ShouldWrite(batch) {
					batchesToApply = append(batchesToApply, batch)
					batches = make([]*Batch, 0)
				}
			}

			// Batches to be applied are forwarded to a separate goroutine responsible for writing to postgres.
			for _, batch := range batchesToApply {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batchChannel <- batch:
				}
			}

		case <-ticker.C: // Periodically send batches to be applied to postgres.
			for _, batch := range batches {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batchChannel <- batch:
				}
			}
			batches = make([]*Batch, 0)
		}
	}
}

func (srv *Ingester) ProcessBatches(ctx context.Context, batchChannel <-chan *Batch) error {
	log := ctxlogrus.Extract(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batchChannel:

			err := srv.ProcessBatch(ctx, batch)
			if err != nil {
				return err // TODO: Keep retrying on transient errors.
			}
			log.Infof("wrote batch records into postgres")

			// Ack all messages that were used to create the batch.
			// Acking is only safe once data has been written to postgres.
			for _, messageId := range batch.MessageIds {
				err := srv.consumer.AckID(messageId)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		}
	}
}
