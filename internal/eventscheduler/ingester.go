package eventscheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/jackc/pgx/v4"
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

// Service responsible for writing records derived from pulsar messages into postgres.
//
// At a high level, the ingester:
// 1. Reads messages from pulsar, which are used to create records.
// 2. Records are collected in a batch until some amount of time has passed or some number of records have been collected.
// 3. The records that make up the batch are upserted into postgres in a single operation.
// 4. The pulsar messages read to produce the batch are acked.
//
// Upserts are idempotent. Specifically, stale writes are detected
// (i.e., if the data in postgres is more recent than the data to be written.).
//
// On stale write, the ingester seeks to the correct position for each topic partition
// before resuming reading. In this way, we ensure exactly once semantics when writing into postgres.
//
// Each ingester instance can only write into a single postgres table.
// I.e., to populate multiple tables with different record types (e.g., jobs and leases),
// a separate ingester instance is required for each record type.
type Ingester struct {
	// Name of Pulsar topic and consumer on which to receive messages.
	topic string
	// Postgres table to write records into.
	table string
	// Schema of the table data is written into.
	// Required for UpsertRecords; see this function for docs.
	schema string
	// Pulsar consumer on which to receive messages.
	consumer pulsar.Consumer
	// Write to postgres at least this often (assuming there are records to write).
	maxWriteInterval time.Duration
	// Write current batch to postgres if at least this many records have been written to it.
	maxWriteRecords int
	// Connection to the postgres database used for persistence.
	db *pgx.Conn
	// Optional logger.
	// If not provided, the default logrus logger is used.
	Logger *logrus.Entry
}

// Batch of records to be written to postgres in bulk.
type Batch struct {
	// Time at which this batch was created.
	createdAt time.Time
	// Ids of messages processed to create this batch.
	MessageIds []pulsar.MessageID
	// Records to be written on the next write to postgres.
	records []interface{}
	// The message id of the most recent message seen for each partition.
	// Used to detect stale writes when writing to postgres,
	// i.e., detecting if the data stored in postgres is more recent than what we're trying to write.
	lastMessageIdByPartition map[int32]pulsar.MessageID
	// The message id of the first message we've seen for each partition.
	// Necessary in case of a stale write, in which case we may need to seek to the start for some partitions.
	firstMessageIdByPartition map[int32]pulsar.MessageID
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
	if time.Since(batch.createdAt) > srv.maxWriteInterval {
		return true
	}
	if len(batch.records) > srv.maxWriteRecords {
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
		err := g.Wait()

		// If the error is of type ErrStaleWrite, we've tried to write stale data into postgres.
		// To resolve, we need to seek all partitions that made up the batch to the
		// - partitions corresponding to stale writes to the id stored in postgres and
		// - all other partitions to the id of the first message for that partition in the batch.
		var e *ErrStaleBatch
		if errors.As(err, e) {

			// Map partition indices to the message id to seek to for all stale partitions.
			staleWriteByPartition := make(map[int32]pulsar.MessageID)
			for _, staleWrite := range e.ErrStaleWrite.StaleWrites {
				partitionIdx := staleWrite.DbMessageId.PartitionIdx()
				if staleWrite.WriteMessageId.PartitionIdx() != partitionIdx {
					return errors.Errorf("mismatched message ids %v", staleWrite)
				}
				if _, ok := staleWriteByPartition[partitionIdx]; ok {
					return errors.Errorf("duplicate StaleWrite %v for partition %d", staleWrite, partitionIdx)
				}
				staleWriteByPartition[partitionIdx] = staleWrite
			}

			// Seek non-stale partitions to the start of the batch.
			// Because we don't know how which messages are in the pipeline,
			// the only safe option is to discard all data not yet written to postgres and starting over.
			//
			// TODO: It may be even better to get ids out of postgres and seek to those.
			// TODO: That could be achieved by bundling the
			for partitionIdx, messageId := range e.Batch.firstMessageIdByPartition {
				if _, ok := staleWriteByPartition[partitionIdx]; !ok {
					srv.consumer.Seek(messageId)
				}
			}

			// Seek stale partitions to the id stored in postgres.

			// For each stale
		} else if err != nil {
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
			createdAt:                 time.Now(),
			MessageIds:                make([]pulsar.MessageID, 0),
			records:                   make([]interface{}, 0),
			firstMessageIdByPartition: make(map[int32]pulsar.MessageID),
			lastMessageIdByPartition:  make(map[int32]pulsar.MessageID),
		}
	}

	// Store the message id in the batch.
	// So we can ack messages once they've been written to postgres.
	//
	// It's fine to write to postgres and then not ack, since stale writes are detected.
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
			messageIndexPointer := msg.Index()
			var messageIndex uint64
			if messageIndexPointer != nil {
				messageIndex = *messageIndexPointer
			}
			job := Job{
				Jobid:        armadaevents.UuidFromProtoUuid(submitJob.JobId),
				Jobset:       sequence.GetJobSetName(),
				Queue:        sequence.GetQueue(),
				Priority:     int64(submitJob.Priority),
				Message:      msg.Payload(),
				Messageindex: int64(messageIndex), // TODO: Possible precision problem.
			}

			// Any consumer can seek on any topic partition.
			// An unexpected seek may cause messages to be delivered out of order,
			// i.e., a received messages may not be newer than the previous message.
			//
			// We compare the id of each received message with that of the previous message
			// on the same partition to detect such out-of-order messages.
			//
			// Note that, while possible, this should be a rare occurrence.
			partitionIdx := msg.ID().PartitionIdx()
			if lastMessageId, ok := batch.lastMessageIdByPartition[partitionIdx]; ok {
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

			// Update the batch.
			batch.MessageIds = append(batch.MessageIds, msg.ID())
			batch.records = append(batch.records, job)
			if _, ok := batch.firstMessageIdByPartition[partitionIdx]; !ok {
				batch.firstMessageIdByPartition[partitionIdx] = msg.ID()
			}
			batch.lastMessageIdByPartition[partitionIdx] = msg.ID()
		}
	}

	return batch, nil
}

func (srv *Ingester) ProcessBatches(ctx context.Context, batchChannel <-chan *Batch) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batchChannel:
			// Write records into postgres.
			err := UpsertRecords(ctx, srv.db, srv.topic, batch.lastMessageIdByPartition, srv.table, srv.schema, batch.records)

			// Detect stale writes and bundle the batch with the ErrStaleWrite.
			var e *ErrStaleWrite
			if errors.As(err, e) {
				return errors.WithStack(&ErrStaleBatch{
					Batch:         batch,
					ErrStaleWrite: e,
				})
			} else if err != nil {
				return err
			}

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

// func (srv *Ingester) Init(ctx context.Context) error {
// 	log := ctxlogrus.Extract(ctx)

// 	// Each write to postgres is based on several messages read from several topics.
// 	// For each such write, we write to postgres the id of the most recent Pulsar message
// 	// for each topic (omitting any topics that didn't contribute to this write).
// 	//
// 	// Here, at startup, we read the message id stored in postgres for each partition and seek to those ids.
// 	// We perform the same check for each write to postgres; doing it here as well is just an optimisation.
// 	sqlMessageIds, err := srv.db.GetTopicMessageIds(ctx, srv.topic)
// 	if err != nil {
// 		return errors.WithStack(err)
// 	}

// 	// Here, we need to seek on each of the individual partitions.
// 	// When we write, we need to compare against all the individual partitions.
// 	// We can write the partitions that weren't stale and then re-seek the others.
// 	srv.consumer.Seek()

// 	messageId := pulsarutils.New(
// 		sqlMessageId.Ledgerid,
// 		sqlMessageId.Entryid,
// 		sqlMessageId.Partitionidx,
// 		sqlMessageId.Batchidx,
// 	)
// 	err = srv.consumer.Seek(messageId)
// 	if err != nil {
// 		return errors.WithStack(err)
// 	}
// }

// SeekPartitions seeks individual partitions of a topic.
func SeekPartitions(ctx context.Context, client pulsar.Client, consumer pulsar.Consumer, topic string, messageIds []pulsar.MessageID) (pulsar.Consumer, error) {
	// log := ctxlogrus.Extract(ctx)

	// Check that there isn't more than one message id for any partition.
	messageIdByPartition := make(map[int32]pulsar.MessageID)
	for _, messageId := range messageIds {
		if _, ok := messageIdByPartition[messageId.PartitionIdx()]; ok {
			return nil, errors.Errorf("duplicate message id for %s-%d", topic, messageId.PartitionIdx())
		}
		messageIdByPartition[messageId.PartitionIdx()] = messageId
	}

	// // Give up the original subscription to allow those created above to take over.
	// err := consumer.Unsubscribe()
	// if err != nil {
	// 	return nil, errors.WithStack(err)
	// }

	// A Pulsar topic partition is just a topic with a special name.
	// I.e., a topic the name of which is suffixed with the partition index.
	// Here, we create consumers for each topic that just do a seek followed by an unsubscribe.
	g, _ := errgroup.WithContext(ctx)
	for _, messageId := range messageIdByPartition {
		messageId := messageId
		err := consumer.Seek(messageId)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		g.Go(func() error {
			// partitionTopic := fmt.Sprintf("%s-partition-%d", topic, messageId.PartitionIdx())
			// consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			// 	Topic:            partitionTopic,
			// 	SubscriptionName: consumer.Subscription(),
			// 	Type:             pulsar.KeyShared,
			// })
			// if err != nil {
			// 	fmt.Println("=============== subscribe failed")
			// 	return errors.WithStack(err)
			// }
			// defer func() {
			// 	err := consumer.Unsubscribe()
			// 	if err != nil {
			// 		err = errors.WithStack(err)
			// 		logging.WithStacktrace(log, err).Errorf("+++++++++++++++++++++++++++ failed to unsubscribe to topic %s on subscription %s", partitionTopic, consumer.Subscription())
			// 	}
			// }()
			// err = consumer.Seek(messageId)
			// if err != nil {
			// 	fmt.Println("=============== seek failed")
			// 	return errors.WithStack(err)
			// }
			return nil
		})
	}

	// Wait for the seek to complete.
	err := g.Wait()
	if err != nil {
		return nil, err
	}

	time.Sleep(10 * time.Second)

	// // Re-subscribe to the partitioned topic.
	// // Note that this may result in being given a different set of partitions than what the original consumer held.
	// fmt.Println("===================== re-subscribing on ", consumer.Subscription(), " - ", topic)
	// newConsumer, err := client.Subscribe(pulsar.ConsumerOptions{
	// 	Topic:            topic,
	// 	SubscriptionName: consumer.Subscription(),
	// 	Type:             pulsar.KeyShared,
	// })
	// if err != nil {
	// 	return nil, errors.WithStack(err)
	// }

	return consumer, nil
}
