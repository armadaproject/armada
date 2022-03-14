package server

import (
	"context"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/internal/events"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/internal/pulsarutils/pulsarrequestid"
)

// PulsarFromPulsar is a service that reads from Pulsar and sends any required new messages.
type PulsarFromPulsar struct {
	Consumer pulsar.Consumer
	Producer pulsar.Producer
	// Logger from which the loggers used by this service are derived
	// (e.g., using srv.Logger.WithField), or nil, in which case the global logrus logger is used.
	Logger *logrus.Entry
}

// Run the service that reads from Pulsar and updates Armada until the provided context is cancelled.
func (srv *PulsarFromPulsar) Run(ctx context.Context) {

	// Get the configured logger, or the standard logger if none is provided.
	var log *logrus.Entry
	if srv.Logger != nil {
		log = srv.Logger.WithField("service", "PulsarFromPulsar")
	} else {
		log = logrus.StandardLogger().WithField("service", "PulsarFromPulsar")
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

	// Periodically log the number of processed messages.
	logInterval := 10 * time.Second
	lastLogged := time.Now()
	numReceived := 0
	numErrored := 0
	var lastMessageId pulsar.MessageID
	lastMessageId = nil
	lastPublishTime := time.Now()

	// Run until ctx is cancelled.
	for {

		// Periodic logging.
		if time.Since(lastLogged) > logInterval {
			log.WithFields(
				logrus.Fields{
					"received":      numReceived,
					"succeeded":     numReceived - numErrored,
					"errored":       numErrored,
					"interval":      logInterval,
					"lastMessageId": lastMessageId,
					"timeLag":       time.Now().Sub(lastPublishTime),
				},
			).Info("message statistics")
			numReceived = 0
			numErrored = 0
			lastLogged = time.Now()
		}

		// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
		select {
		case <-ctx.Done():
			return
		default:

			// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
			ctxWithTimeout, _ := context.WithTimeout(ctx, 10*time.Second)
			msg, err := srv.Consumer.Receive(ctxWithTimeout)
			if errors.Is(err, context.DeadlineExceeded) {
				break //expected
			}

			// If receiving fails, try again in the hope that the problem is transient.
			// We don't need to distinguish between errors here, since any error means this function can't proceed.
			if err != nil {
				logging.WithStacktrace(log, err).WithField("lastMessageId", lastMessageId).Warnf("Pulsar receive failed; backing off")
				time.Sleep(100 * time.Millisecond)
				continue
			}
			lastMessageId = msg.ID()
			lastPublishTime = msg.PublishTime()
			numReceived++

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
			sequence, err := UnmarshalEventSequence(ctxWithLogger, msg.Payload())
			if err != nil {
				// If unmarshalling fails, the message is malformed and we have no choice but to ignore it.
				// TODO: Put the message on a special topic for later analysis.
				logging.WithStacktrace(messageLogger, err).Warnf("processing message failed; ignoring")
				numErrored++
				continue
			}

			// Process the events in the sequence. For efficiency, we may process several events at a time.
			// TODO: Determine what to do based on the error (e.g., publish to a dead letter topic).
			messageLogger.WithField("numEvents", len(sequence.Events)).Info("processing sequence")
			err = srv.ProcessSequence(ctx, sequence)
			for pulsarutils.IsPulsarError(err) {
				time.Sleep(time.Second)
				err = srv.ProcessSequence(ctx, sequence)
			}
			if err != nil {
				logging.WithStacktrace(messageLogger, err).Error("failed to process sequence")
			}
		}
	}
}

func (srv *PulsarFromPulsar) ProcessSequence(ctx context.Context, sequence *events.EventSequence) error {

	// Get any responses that should be sent in response to these events.
	es := srv.ResponseEventsFromSequence(ctx, sequence)
	if len(es) == 0 {
		return nil
	}

	// Send the resulting events.
	payload, err := proto.Marshal(&events.EventSequence{
		Queue:      sequence.Queue,
		JobSetName: sequence.JobSetName,
		Events:     es,
	})
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	// Get a request id embedded in the context.
	requestId := requestid.FromContextOrMissing(ctx)

	// Prepare message with embedded request id.
	msg := &pulsar.ProducerMessage{
		Payload: payload,
		Key:     sequence.JobSetName,
	}
	pulsarrequestid.AddToMessage(msg, requestId)

	ctxWithTimeout, _ := context.WithTimeout(ctx, 30*time.Second)
	_, err = srv.Producer.Send(ctxWithTimeout, msg)
	if err != nil {
		err = errors.WithStack(err)
		return err
	}

	return nil
}

// ResponseEventsFromSequence returns a slice with all events that should be sent in response to the provided sequence.
func (srv *PulsarFromPulsar) ResponseEventsFromSequence(ctx context.Context, sequence *events.EventSequence) []*events.EventSequence_Event {
	es := make([]*events.EventSequence_Event, 0)
	for i := 0; i < len(sequence.Events); i++ {
		switch e := sequence.Events[i].Event.(type) {
		// In case of a JobRunSucceeded message, mark the job as succeeded by sending a JobSucceeded message.
		// This is not strictly according to spec, since there may be other active job runs for the same job.
		// Ideally, we would make sure there are no other such runs before marking the job as succeeded.
		case *events.EventSequence_Event_JobRunSucceeded:
			es = append(es, &events.EventSequence_Event{
				Event: &events.EventSequence_Event_JobSucceeded{
					JobSucceeded: &events.JobSucceeded{
						JobId: e.JobRunSucceeded.JobId,
					},
				},
			})
		default:
			// Event not handled by this method; ignore.
		}
	}
	return es
}
