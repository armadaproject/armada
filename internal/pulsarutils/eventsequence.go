package pulsarutils

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/requestid"
	"github.com/G-Research/armada/pkg/armadaevents"
)

// CompactAndPublishSequences reduces the number of sequences to the smallest possible,
// while respecting per-job set ordering and max Pulsar message size, and then publishes to Pulsar.
func CompactAndPublishSequences(ctx context.Context, sequences []*armadaevents.EventSequence, producer pulsar.Producer, maxMessageSizeInBytes int) error {
	// Reduce the number of sequences to send to the minimum possible,
	// and then break up any sequences larger than maxMessageSizeInBytes.
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, maxMessageSizeInBytes, true)
	if err != nil {
		return err
	}
	return PublishSequences(ctx, producer, sequences)
}

// PublishSequence publishes several event sequences to Pulsar.
// For efficiency, all sequences are queued for publishing and then flushed.
// Returns once all sequences have been received by Pulsar.
//
// To reduce the number of separate sequences sent and ensure limit message size, call
// eventutil.CompactEventSequences(sequences)
// and
// eventutil.LimitSequencesByteSize(sequences, int(srv.MaxAllowedMessageSize))
// before passing to this function.
func PublishSequences(ctx context.Context, producer pulsar.Producer, sequences []*armadaevents.EventSequence) error {
	// Incoming gRPC requests are annotated with a unique id.
	// Pass this id through the log by adding it to the Pulsar message properties.
	requestId := requestid.FromContextOrMissing(ctx)

	// Send each sequence async. Collect any errors via ch.
	ch := make(chan error, len(sequences))
	defer close(ch)
	for _, sequence := range sequences {
		payload, err := proto.Marshal(sequence)
		if err != nil {
			return errors.WithStack(err)
		}

		producer.SendAsync(
			ctx,
			&pulsar.ProducerMessage{
				Payload: payload,
				Properties: map[string]string{
					requestid.MetadataKey:                     requestId,
					armadaevents.PULSAR_MESSAGE_TYPE_PROPERTY: armadaevents.PULSAR_CONTROL_MESSAGE,
				},
				Key: sequence.JobSetName,
			},
			// Callback on send.
			func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				ch <- err
			},
		)
	}

	// Flush queued messages and wait until persisted.
	err := producer.Flush()
	if err != nil {
		return errors.WithStack(err)
	}

	// Collect any errors experienced by the async send and return.
	var result *multierror.Error
	for range sequences {
		result = multierror.Append(result, <-ch)
	}
	return result.ErrorOrNil()
}
