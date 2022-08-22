package eventscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"github.com/severinson/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// func NewJobsIngester(topic string, ) *Ingester {
// 	return &Ingester{

// 	}
// }

// Service responsible for writing records derived from pulsar messages into postgres.
//
// At a high level, the ingester:
// 1. Reads messages from pulsar, which are used to create records.
// 2. Records are collected in a batch until some amount of time has passed or some number of records have been collected.
// 3. The records that make up the batch are bulk upserted into postgres.
// 4. The pulsar messages read to produce the batch are acked.
//
// Because we just store records, upserts are idempotent.
//
// Each ingester instance can only write into a single postgres table.
// I.e., to populate multiple tables with different record types (e.g., jobs and leases),
// a separate ingester instance is required for each record type.
type Ingester struct {
	// Used to setup a Pulsar consumer.
	PulsarClient    pulsar.Client
	ConsumerOptions pulsar.ConsumerOptions
	// Postgres Table to write records into.
	Table string
	// Schema of the table data is written into.
	// Required for UpsertRecords; see this function for docs.
	Schema string
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

// Batch of records to be written to postgres in bulk.
type Batch struct {
	// Time at which this batch was created.
	createdAt time.Time
	// Ids of messages processed to create this batch.
	// Note that these don't map one-to-one to records,
	// since only a subset of the messages may have resulted in a record being created.
	MessageIds []pulsar.MessageID
	// Records to be written on the next write to postgres.
	records []interface{}
}

// ErrStaleBatch indicates that some (or all) records in batch are derivded from partitions
// for which the data stored in postgres is more recent.
type ErrStaleBatch struct {
	Batch         *Batch
	ErrStaleWrite *ErrStaleWrite
}

func (err *ErrStaleBatch) Error() string {
	return fmt.Sprintf("stale write for batch %+v: %s", err.Batch, err.ErrStaleWrite)
}

// ShouldWrite returns true if this batch should be written into postgres.
func (srv *Ingester) ShouldWrite(batch *Batch) bool {
	if batch == nil {
		return false
	}
	if time.Since(batch.createdAt) > srv.MaxWriteInterval {
		return true
	}
	if len(batch.records) > srv.MaxWriteRecords {
		return true
	}
	return false
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
	//
	// On ErrStaleWrite:
	// 1. Stop and tear down the pipeline.
	// 2. Seek each partition to the correct position.
	// 3. Setup a new pipeline and start again.
	//
	// On any other error, return the error to the caller of this function.
	for {

		// Scheduler ingester
		consumer, err := srv.PulsarClient.Subscribe(srv.ConsumerOptions)
		if err != nil {
			return errors.WithStack(err)
		}
		defer consumer.Close()
		srv.consumer = consumer

		// All services run within an errgroup.
		g, ctx := errgroup.WithContext(ctx)
		ctx = ctxlogrus.ToContext(ctx, log)

		// Receive Pulsar messages asynchronously.
		pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.consumer)
		g.Go(func() error { return pulsarToChannel.Run(ctx) })

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

	// In-progress batch. Initialized automatically by ProcessMessage.
	var batch *Batch

	// Ticker to trigger checking if we should write the current batch to postgres.
	ticker := time.NewTicker(time.Second)
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

			var err error
			batch, err = srv.ProcessMessage(ctxWithLogger, msg, batch)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
			}
		case <-ticker.C: // Periodically check if we should write to postgres.
			if srv.ShouldWrite(batch) {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case batchChannel <- batch:
					batch = nil
				}
			}
		}
	}
}

func (srv *Ingester) ProcessMessage(ctx context.Context, msg pulsar.Message, batch *Batch) (*Batch, error) {
	if msg == nil {
		return batch, nil
	}

	// Create a fresh batch if we don't already have one.
	if batch == nil {
		batch = &Batch{
			createdAt:  time.Now(),
			MessageIds: make([]pulsar.MessageID, 0),
			records:    make([]interface{}, 0),
		}
	}

	// Store the message id in the batch.
	// So we can ack messages once they've been written to postgres.
	//
	// It's fine to write to postgres and then not ack, since writes are idempotent.
	// But it's not fine to ack messages not written to postgres,
	// since skipping messages isn't detected.
	batch.MessageIds = append(batch.MessageIds, msg.ID())

	// Unmarshal and validate the message.
	sequence, err := eventutil.UnmarshalEventSequence(ctx, msg.Payload())
	if err != nil {
		return batch, err
	}
	if sequence == nil || len(sequence.Events) == 0 {
		return batch, nil
	}

	for _, event := range sequence.GetEvents() {
		if eventSubmitJob, ok := event.Event.(*armadaevents.EventSequence_Event_SubmitJob); ok {
			submitJob := eventSubmitJob.SubmitJob
			job := Job{
				JobID:         armadaevents.UuidFromProtoUuid(submitJob.JobId),
				JobSet:        sequence.GetJobSetName(),
				Queue:         sequence.GetQueue(),
				Priority:      int64(submitJob.Priority),
				SubmitMessage: msg.Payload(),
			}

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
					return batch, err
				}
				if msgIsOufOfOrder {
					// pulsarutils.PulsarMessageId prints nicely, so we convert to those.
					return batch, fmt.Errorf(
						"unexpected seek detected: received messages out of order for topic %s: id of received message is %s, but the previous message has id %s",
						msg.Topic(), pulsarutils.FromMessageId(msg.ID()), pulsarutils.FromMessageId(lastMessageId),
					)
				}
			}
			srv.lastMessageIdByPartition[partitionIdx] = msg.ID()

			// Update the batch.
			batch.MessageIds = append(batch.MessageIds, msg.ID())
			batch.records = append(batch.records, job)
		}
	}

	return batch, nil
}

func (srv *Ingester) ProcessBatches(ctx context.Context, batchChannel <-chan *Batch) error {
	log := ctxlogrus.Extract(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batchChannel:
			// Write records into postgres.
			err := Upsert(ctx, srv.Db, srv.Table, srv.Schema, batch.records)
			if err != nil {
				return err // TODO: Keep retrying on transient faults.
			}
			log.Infof("wrote %d records into postgres", len(batch.records))

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
