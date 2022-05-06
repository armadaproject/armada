package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// EventsPrinter is a service that prints all events passing through pulsar to a logger.
// This service is only meant for use during development; it will be slow when the number of events is large.
type EventsPrinter struct {
	Consumer pulsar.Consumer
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
}

// Run the service that reads from Pulsar and updates Armada until the provided context is cancelled.
func (srv *EventsPrinter) Run(ctx context.Context) {

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
			go srv.Run(ctx)
		} else {
			// An expected shutdown.
			log.Info("service stopped")
		}
	}()

	// Run until ctx is cancelled.
	for {

		// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
		select {
		case <-ctx.Done():
			return
		default:

			// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
			ctxWithTimeout, _ := context.WithTimeout(ctx, 10*time.Second)
			msg, err := srv.Consumer.Receive(ctxWithTimeout)
			if errors.Is(err, context.DeadlineExceeded) { //expected
				log.Info("no new messages from Pulsar")
				break
			} else if err != nil {
				logging.WithStacktrace(log, err).Warnf("receiving from Pulsar failed")
				break
			}

			srv.Consumer.Ack(msg)

			// We're only interested in control messages.
			if !armadaevents.IsControlMessage(msg) {
				continue
			}

			sequence := &armadaevents.EventSequence{}
			err = proto.Unmarshal(msg.Payload(), sequence)
			if err != nil {
				logging.WithStacktrace(log, err).Warnf("unmarshalling Pulsar message failed")
				continue
			}

			messageLogger := log.WithFields(logrus.Fields{
				"Queue":               sequence.Queue,
				"JobSetName":          sequence.JobSetName,
				"UserId":              sequence.UserId,
				"Groups":              sequence.Groups,
				"NumEvents":           len(sequence.Events),
				requestid.MetadataKey: pulsarrequestid.FromMessageOrMissing(msg),
			})

			s := "Sequence: "
			for _, event := range sequence.Events {
				jobId, _ := armadaevents.JobIdFromEvent(event)
				jobIdString, err := armadaevents.UlidStringFromProtoUuid(jobId)
				if err != nil {
					logging.WithStacktrace(log, err).Warnf("converting jobId to string failed")
					jobIdString = ""
				}
				s += fmt.Sprintf("[%T (job %s)] ", event.Event, jobIdString)
			}

			messageLogger.Info(s)
		}
	}
}
