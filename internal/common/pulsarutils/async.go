package pulsarutils

import (
	gocontext "context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/context"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
)

// ConsumerMessageId wraps a pulsar message id  and an identifier for the consumer which originally received the
// corresponding message.  This exists because we need to track which messages came from which consumers so that
// we can ACK them on the correct consumer.
type ConsumerMessageId struct {
	MessageId  pulsar.MessageID
	Index      int64
	ConsumerId int
}

// ConsumerMessage wraps a pulsar message and an identifier for the consumer which originally received the
// corresponding message.  This exists because we need to track which messages came from which consumers so that
// we can ACK them on the correct consumer.
type ConsumerMessage struct {
	Message    pulsar.Message
	ConsumerId int
}

var msgLogger = logrus.NewEntry(logrus.StandardLogger())

func Receive(
	ctx *context.ArmadaContext,
	consumer pulsar.Consumer,
	receiveTimeout time.Duration,
	backoffTime time.Duration,
	m *commonmetrics.Metrics,
) chan pulsar.Message {
	out := make(chan pulsar.Message)
	go func() {
		// Periodically log the number of processed messages.
		logInterval := 60 * time.Second
		lastLogged := time.Now()
		numReceived := 0
		var lastMessageId pulsar.MessageID
		lastMessageId = nil
		lastPublishTime := time.Now()

		// Run until ctx is cancelled.
		for {
			// Periodic logging.
			if time.Since(lastLogged) > logInterval {
				msgLogger.WithFields(
					logrus.Fields{
						"received":      numReceived,
						"interval":      logInterval,
						"lastMessageId": lastMessageId,
						"timeLag":       time.Now().Sub(lastPublishTime),
					},
				).Info("message statistics")
				numReceived = 0
				lastLogged = time.Now()
			}

			// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
			select {
			case <-ctx.Done():
				msgLogger.Infof("Shutting down pulsar receiver")
				close(out)
				return
			default:
				// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
				ctxWithTimeout, cancel := context.WithTimeout(ctx, receiveTimeout)
				msg, err := consumer.Receive(ctxWithTimeout)
				if errors.Is(err, gocontext.DeadlineExceeded) {
					msgLogger.Debugf("No message received")
					cancel()
					break // expected
				}
				cancel()
				// If receiving fails, try again in the hope that the problem is transient.
				// We don't need to distinguish between errors here, since any error means this function can't proceed.
				if err != nil {
					m.RecordPulsarConnectionError()
					logging.
						WithStacktrace(msgLogger, err).
						WithField("lastMessageId", lastMessageId).
						Warnf("Pulsar receive failed; backing off for %s", backoffTime)
					time.Sleep(backoffTime)
					continue
				}

				numReceived++
				lastPublishTime = msg.PublishTime()
				lastMessageId = msg.ID()
				out <- msg
			}
		}
	}()
	return out
}

// Ack will ack all pulsar messages coming in on the msgs channel. The incoming messages contain a consumer id which
// corresponds to the index of the consumer that should be used to perform the ack.  In theory, the acks could be done
// in parallel, however its unlikely that they will be a performance bottleneck
func Ack(ctx *context.ArmadaContext, consumers []pulsar.Consumer, msgs chan []*ConsumerMessageId, backoffTime time.Duration, wg *sync.WaitGroup) {
	for msg := range msgs {
		for _, id := range msg {
			if id.ConsumerId < 0 || id.ConsumerId >= len(consumers) {
				// This indicates a programming error and should never happen!
				panic(
					fmt.Sprintf(
						"Asked to ack message belonging to consumer %d, however this is outside the bounds of the consumers array which is of length %d",
						id.ConsumerId, len(consumers)))
			}
			util.RetryUntilSuccess(
				ctx,
				func() error { return consumers[id.ConsumerId].AckID(id.MessageId) },
				func(err error) {
					logging.
						WithStacktrace(msgLogger, err).
						WithField("lastMessageId", id.MessageId).
						Warnf("Pulsar ack failed; backing off for %s", backoffTime)
					time.Sleep(backoffTime)
				},
			)
		}
	}
	msgLogger.Info("Shutting down Ackker")
	wg.Done()
}
