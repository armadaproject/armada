package pulsarutils

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/logging"
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

var log = logrus.NewEntry(logrus.StandardLogger())

// Receive returns a channel containing messages received from pulsar.  This channel will remain open until the
// supplied context is closed.
// consumerId: Internal Id of the consumer.  We use this so that when messages from different consumers are multiplexed, we know which messages originated form which consumers
// bufferSize: sets the size of the buffer in the returned channel
// receiveTimeout: sets how long the pulsar consumer will wait for a message before retrying
// backoffTime: sets how long the consumer will wait before retrying if the pulsar consumer indicates an error receiving from pulsar.
func Receive(ctx context.Context, consumer pulsar.Consumer, consumerId int, bufferSize int, receiveTimeout time.Duration, backoffTime time.Duration) chan *ConsumerMessage {
	out := make(chan *ConsumerMessage, bufferSize)
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
				log.WithFields(
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
				log.Infof("Shutting down pulsar receiver %d", consumerId)
				close(out)
				return
			default:

				// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
				ctxWithTimeout, cancel := context.WithTimeout(ctx, receiveTimeout)
				msg, err := consumer.Receive(ctxWithTimeout)
				if errors.Is(err, context.DeadlineExceeded) {
					log.Debugf("No message received")
					cancel()
					break // expected
				}
				cancel()
				// If receiving fails, try again in the hope that the problem is transient.
				// We don't need to distinguish between errors here, since any error means this function can't proceed.
				if err != nil {
					logging.
						WithStacktrace(log, err).
						WithField("lastMessageId", lastMessageId).
						Warnf("Pulsar receive failed; backing off for %s", backoffTime)
					time.Sleep(backoffTime)
					continue
				}

				numReceived++
				lastPublishTime = msg.PublishTime()
				lastMessageId = msg.ID()
				out <- &ConsumerMessage{
					Message:    msg,
					ConsumerId: consumerId,
				}
			}
		}
	}()
	return out
}

// Ack will ack all pulsar messages coming in on the msgs channel. The incoming messages contain a consumer id which
// corresponds to the index of the consumer that should be used to perform the ack.  In theory, the acks could be done
// in parallel, however its unlikely that they will be a performance bottleneck
func Ack(ctx context.Context, consumers []pulsar.Consumer, msgs chan []*ConsumerMessageId, wg *sync.WaitGroup) {
	for msg := range msgs {
		for _, id := range msg {
			if id.ConsumerId < 0 || id.ConsumerId >= len(consumers) {
				// This indicates a programming error and should never happen!
				panic(
					fmt.Sprintf(
						"Asked to ack message belonging to consumer %d, however this is outside the bounds of the consumers array which is of length %d",
						id.ConsumerId, len(consumers)))
			}
			consumers[id.ConsumerId].AckID(id.MessageId)
		}
	}
	log.Info("Shutting down Ackker")
	wg.Done()
}
