package server

import (
	gocontext "context"
	"errors"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/pulsarutils/pulsarrequestid"
	"github.com/armadaproject/armada/internal/common/requestid"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// EventsPrinter is a service that prints all events passing through pulsar to a logger.
// This service is only meant for use during development; it will be slow when the number of events is large.
type EventsPrinter struct {
	Client           pulsar.Client
	Topic            string
	SubscriptionName string
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
}

// Run the service that reads from Pulsar and updates Armada until the provided context is cancelled.
func (srv *EventsPrinter) Run(ctx *armadacontext.ArmadaContext) error {
	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "EventsPrinter")
	} else {
		log = logrus.StandardLogger().WithField("service", "EventsPrinter")
	}
	log.Info("service started")

	// Recover from panics by restarting the service.
	defer func() {
		if err := recover(); err != nil {
			log.WithField("error", err).Error("unexpected panic; restarting")
			time.Sleep(time.Second)
			go func() {
				if err := srv.Run(ctx); err != nil {
					logging.WithStacktrace(log, err).Error("eventsprinter failure")
				}
			}()
		} else {
			// An expected shutdown.
			log.Info("service stopped")
		}
	}()

	consumer, err := srv.Client.Subscribe(pulsar.ConsumerOptions{
		Topic:            srv.Topic,
		SubscriptionName: srv.SubscriptionName,
		Type:             pulsar.Failover,
	})
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// Run until ctx is cancelled.
	for {
		// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
		select {
		case <-ctx.Done():
			return nil
		default:

			// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
			ctxWithTimeout, cancel := armadacontext.WithTimeout(ctx, 10*time.Second)
			msg, err := consumer.Receive(ctxWithTimeout)
			cancel()
			if errors.Is(err, gocontext.DeadlineExceeded) { // expected
				log.Info("no new messages from Pulsar (or another instance holds the subscription)")
				break
			} else if err != nil {
				logging.WithStacktrace(log, err).Warnf("receiving from Pulsar failed")
				break
			}
			util.RetryUntilSuccess(
				armadacontext.Background(),
				func() error { return consumer.Ack(msg) },
				func(err error) {
					logging.WithStacktrace(log, err).Warnf("acking pulsar message failed")
					time.Sleep(time.Second) // Not sure what the right backoff is here
				},
			)

			sequence := &armadaevents.EventSequence{}
			if err := proto.Unmarshal(msg.Payload(), sequence); err != nil {
				logging.WithStacktrace(log, err).Warnf("unmarshalling Pulsar message failed")
				break
			}

			messageLogger := log.WithFields(logrus.Fields{
				"Queue":               sequence.Queue,
				"JobSetName":          sequence.JobSetName,
				"UserId":              sequence.UserId,
				"Groups":              sequence.Groups,
				"NumEvents":           len(sequence.Events),
				requestid.MetadataKey: pulsarrequestid.FromMessageOrMissing(msg),
				"PublishTime":         msg.PublishTime(),
				"EventTime":           msg.EventTime(),
				"Topic":               msg.Topic(),
				"Properties":          msg.Properties(),
				"PulsarId":            msg.ID(),
				"Key":                 msg.Key(),
			})

			s := "Sequence: " + eventutil.ShortSequenceString(sequence)
			messageLogger.Info(s)
		}
	}
}
