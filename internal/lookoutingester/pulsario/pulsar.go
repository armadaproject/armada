package pulsario

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/lookoutingester/model"
)

var log = logrus.NewEntry(logrus.StandardLogger())

// Receive returns a channel containing messages received from pulsar.  This channel will remain open until the
// supplied context is closed.
// consumerId: Internal Id of the consumer.  We use this so that when messages from different consumers are multiplexed, we know which messages originated form which consumers
// bufferSize: sets the size of the buffer in the returned channel
// receiveTimeout: sets how long the pulsar consumer will wait for a message before retrying
// backoffTime: sets how long the consumer will wait before retrying if the pulsar consumer indicates an error receiving from pulsar.
func Receive(ctx context.Context, consumer pulsar.Consumer, consumerId int, bufferSize int, receiveTimeout time.Duration, backoffTime time.Duration) chan *model.ConsumerMessage {
	out := make(chan *model.ConsumerMessage, bufferSize)
	go func() {
		var lastMessageId pulsar.MessageID

		// Run until ctx is cancelled.
		for {

			// Exit if the context has been cancelled. Otherwise, get a message from Pulsar.
			select {
			case <-ctx.Done():
				log.Infof("Shutting down pulsar receiver %d", consumerId)
				return
			default:

				// Get a message from Pulsar, which consists of a sequence of events (i.e., state transitions).
				ctxWithTimeout, _ := context.WithTimeout(ctx, receiveTimeout)
				msg, err := consumer.Receive(ctxWithTimeout)
				if errors.Is(err, context.DeadlineExceeded) {
					log.Debugf("No message received")
					break //expected
				}

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
				log.Debugf("Recevied message %s", msg.ID())
				lastMessageId = msg.ID()
				out <- &model.ConsumerMessage{
					Message:    msg,
					ConsumerId: consumerId,
				}
			}
		}
		close(out)
	}()
	return out
}

// Ack will ack all pulsar messages coming in on the msgs channel. The incoming messages contain a consumer id which
// corresponds to the index of the consumer that should be used to perform the ack.  In theory, the acks could be done
// in parallel, however its unlikely that they will be a performance bottleneck
func Ack(ctx context.Context, consumers []pulsar.Consumer, msgs chan []*model.ConsumerMessageId, wg *sync.WaitGroup) {
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
