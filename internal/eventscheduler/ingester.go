package eventscheduler

import (
	"context"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
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

			err = srv.ProcessSequence(ctx, sequence)
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				srv.consumer.Ack(msg)
				break
			}
			srv.consumer.Ack(msg)
		}
	}
}

func (srv *Ingester) Init(ctx context.Context) error {
	log := ctxlogrus.Extract(ctx)

	// Each write to postgres is based on several messages read from several topics.
	// For each such write, we write to postgres the id of the most recent Pulsar message
	// for each topic (omitting any topics that didn't contribute to this write).
	//
	// Here, at startup, we read the message id stored in postgres for each partition and seek to those ids.
	// We perform the same check for each write to postgres; doing it here as well is just an optimisation.
	sqlMessageIds, err := srv.db.GetTopicMessageIds(ctx, srv.topic)
	if err != nil {
		return errors.WithStack(err)
	}

	// Here, we need to seek on each of the individual partitions.
	// When we write, we need to compare against all the individual partitions.
	// We can write the partitions that weren't stale and then re-seek the others.
	srv.consumer.Seek()

	messageId := pulsarutils.New(
		sqlMessageId.Ledgerid,
		sqlMessageId.Entryid,
		sqlMessageId.Partitionidx,
		sqlMessageId.Batchidx,
	)
	err = srv.consumer.Seek(messageId)
	if err != nil {
		return errors.WithStack(err)
	}
}
