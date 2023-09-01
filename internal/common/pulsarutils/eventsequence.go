package pulsarutils

import (
	"sync/atomic"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/requestid"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// CompactAndPublishSequences reduces the number of sequences to the smallest possible,
// while respecting per-job set ordering and max Pulsar message size, and then publishes to Pulsar.
func CompactAndPublishSequences(ctx *armadacontext.ArmadaContext, sequences []*armadaevents.EventSequence, producer pulsar.Producer, maxMessageSizeInBytes uint, scheduler schedulers.Scheduler) error {
	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than maxMessageSizeInBytes.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, maxMessageSizeInBytes, true)
	if err != nil {
		return err
	}
	return PublishSequences(ctx, producer, sequences, scheduler)
}

// PublishSequences publishes several event sequences to Pulsar.
// For efficiency, all sequences are queued for publishing and then flushed.
// Returns once all sequences have been received by Pulsar.
//
// To reduce the number of separate sequences sent and ensure limit message size, call
// eventutil.CompactEventSequences(sequences)
// and
// eventutil.LimitSequencesByteSize(sequences, int(srv.MaxAllowedMessageSize))
// before passing to this function.
func PublishSequences(ctx *armadacontext.ArmadaContext, producer pulsar.Producer, sequences []*armadaevents.EventSequence, scheduler schedulers.Scheduler) error {
	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)

	// First, serialise all payloads,
	// to avoid a partial failure where some sequence fails to serialise
	// after other sequences have already been sent.
	payloads := make([][]byte, len(sequences))
	for i, sequence := range sequences {
		if sequence == nil {
			return errors.Errorf("failed to send sequence %v", sequence)
		}
		payload, err := proto.Marshal(sequence)
		if err != nil {
			return errors.WithStack(err)
		}
		payloads[i] = payload
	}

	// Then, send all sequences concurrently (while respecting order),
	// using Pulsar async send. Collect any errors via ch.
	// ch must be buffered to avoid sending on ch blocking,
	// which is not allowed in the callback.
	ch := make(chan error, len(sequences))
	var numSendCompleted uint32
	for i := range sequences {
		producer.SendAsync(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payloads[i],
				Properties: map[string]string{
					requestid.MetadataKey:   requestId,
					schedulers.PropertyName: schedulers.MsgPropertyFromScheduler(scheduler),
				},
				Key: sequences[i].JobSetName,
			},
			// Callback on send.
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				ch <- err

				// The final send to complete is responsible for closing the channel.
				isFinalCallback := atomic.AddUint32(&numSendCompleted, 1) == uint32(len(sequences))
				if isFinalCallback {
					close(ch)
				}
			},
		)
	}

	// Wait for all async send calls to complete, collect any errors, and return.
	var result *multierror.Error
	for range sequences {
		select {
		case <-ctx.Done():
			result = multierror.Append(result, ctx.Err())
			return result.ErrorOrNil()
		case err := <-ch:
			result = multierror.Append(result, err)
		}
	}
	return result.ErrorOrNil()
}
