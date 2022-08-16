package eventscheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
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

// The logic is
// 1. Open a KeyShared subscription.
// 2. Collect stuff for a bit.
// 3. Try to write that to the db. Handle ErrStaleWrite.
//
// On ErrStaleWrite, discard everything, seek the stale partitions to what the error says. Seek everything else to the last successful write.
// Re-start reading from Pulsar.
//
// If handling multiple tables, those need to be processed in the same transaction.
// To ensure the message ids are kept in sync.
// Otherwise I could have two separate consumers, each handling its own table.
// That may be a cleaner approach, actually.
// Since it separates the failure domains.
// The only drawback is having to look over each message sequence twice.
// But that's probably fine.

// Service responsible for populating the underlying postgres database,
// from which the scheduler gets its state, with jobs and runs (i.e., job lease) info.
type Ingester struct {
	// Name of Pulsar topic and consumer on which to receive messages.
	topic    string
	consumer pulsar.Consumer
	// Connection to the postgres database used for persistence.
	db *Queries
	// Data to be written into postgres on the next flush.
	runs map[armadaevents.Uuid]*Run
	jobs map[armadaevents.Uuid]*Job
	// Message ids to ack on successful flush.
	messageIds []pulsar.MessageID
	Logger     *logrus.Entry
	mu         sync.Mutex
}

// Batch of records to be written to postgres in bulk.
type Batch struct {
	// Records to be written on the next write to postgres.
	records []interface{}
	// Message ids to ack on successful write to postgres.
	messageIds []pulsar.MessageID
	// The message id of the first message we've seen for each partition.
	// Necessary in case of a stale write, in which case we may need to seek to the start for some partitions.
	firstMessageIdByPartition map[int32]pulsar.MessageID
}

func NewIngester(topic string, consumer pulsar.Consumer, db DBTX) *Ingester {
	return &Ingester{
		topic:      topic,
		consumer:   consumer,
		db:         &Queries{db: db},
		runs:       make(map[armadaevents.Uuid]*Run),
		jobs:       make(map[armadaevents.Uuid]*Job),
		messageIds: make([]pulsar.MessageID, 0),
	}
}

// Maintain the view of the scheduler state by reading from the log and writing to postgres.
func (srv *Ingester) Run(ctx context.Context) error {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "SchedulerIngester")
	} else {
		log = logrus.StandardLogger().WithField("service", "SchedulerIngester")
	}
	log.Info("service started")

	// Receive Pulsar messages asynchronously.
	g, ctx := errgroup.WithContext(ctx)
	pulsarToChannel := pulsarutils.NewPulsarToChannel(srv.consumer)
	g.Go(func() error { return pulsarToChannel.Run(ctx) })

	// Run until ctx is cancelled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-pulsarToChannel.C:

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

			// Unmarshal and validate the message.
			sequence, err := eventutil.UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				srv.consumer.Ack(msg)
				break
			}
			fmt.Println(sequence)

			// err = srv.ProcessSequence(ctx, sequence)
			// if err != nil {
			// 	logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
			// 	srv.consumer.Ack(msg)
			// 	break
			// }
			srv.consumer.Ack(msg)
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
