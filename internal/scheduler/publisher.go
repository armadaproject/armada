package scheduler

import (
	"context"
	"fmt"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"strconv"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const explicitPartitionKey = "armada_pulsar_partition"

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	// PublishMessages will publish the supplied messages. A LeaderToken is provided and the
	// implementor may decide whether to publish based on the status of this token
	PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error

	// PublishMarkers publishes a single marker message for each Pulsar partition.  Each marker
	// massage contains the supplied group id, which allows all marker messages for a given call
	// to be identified.  The uint32 returned is the number of messages published
	PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error)
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher struct {
	// Used to send messages to pulsar
	producer pulsar.Producer
	// Number of partitions on the pulsar topic
	numPartitions int
	// Timeout after which async messages sends will be considered failed
	pulsarSendTimeout time.Duration
	// Maximum size (in bytes) of produced pulsar messages.
	// This must be below 4MB which is the pulsar message size limit
	maxMessageBatchSize uint
}

func NewPulsarPublisher(
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	pulsarSendTimeout time.Duration,
) (*PulsarPublisher, error) {
	partitions, err := pulsarClient.TopicPartitions(producerOptions.Topic)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	producerOptions.MessageRouter = createMessageRouter(producerOptions)
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &PulsarPublisher{
		producer:            producer,
		pulsarSendTimeout:   pulsarSendTimeout,
		maxMessageBatchSize: 2 * 1024 * 1024, // max pulsar message size is 4MB, so we use 2MB here to be safe
		numPartitions:       len(partitions),
	}, nil
}

// PublishMessages publishes all event sequences to pulsar. Event sequences for a given jobset will be combined into
// single event sequences up to maxMessageBatchSize.
func (p *PulsarPublisher) PublishMessages(ctx context.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error {
	sequences := eventutil.CompactEventSequences(events)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, p.maxMessageBatchSize, true)
	if err != nil {
		return err
	}
	msgs := make([]*pulsar.ProducerMessage, len(sequences))
	for i, sequence := range sequences {
		bytes, err := proto.Marshal(sequence)
		if err != nil {
			return err
		}
		msgs[i] = &pulsar.ProducerMessage{
			Payload: bytes,
			Key:     sequences[i].JobSetName,
			Properties: map[string]string{
				pulsarutils.SchedulerNameKey: pulsarutils.PulsarScheduler,
			},
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(len(msgs))

	// Send messages
	if shouldPublish() {
		log.Debugf("Am leader so will publish")
		sendCtx, cancel := context.WithTimeout(ctx, p.pulsarSendTimeout)
		errored := false
		for _, msg := range msgs {
			p.producer.SendAsync(sendCtx, msg, func(_ pulsar.MessageID, _ *pulsar.ProducerMessage, err error) {
				if err != nil {
					log.WithError(err).Error("error sending message to Pulsar")
					errored = true
				}
				wg.Done()
			})
		}
		wg.Wait()
		cancel()
		if errored {
			return errors.New("One or more messages failed to send to Pulsar")
		}
	} else {
		log.Debugf("No longer leader so not publishing")
	}
	return nil
}

// PublishMarkers sends one pulsar message (containing an armadaevents.PartitionMarker) to each partition
// of the producer's Pulsar topic.
func (p *PulsarPublisher) PublishMarkers(ctx context.Context, groupId uuid.UUID) (uint32, error) {
	for i := 0; i < p.numPartitions; i++ {
		pm := &armadaevents.PartitionMarker{
			GroupId:   armadaevents.ProtoUuidFromUuid(groupId),
			Partition: uint32(i),
		}
		bytes, err := proto.Marshal(pm)
		if err != nil {
			return 0, err
		}
		msg := &pulsar.ProducerMessage{
			Properties: map[string]string{
				explicitPartitionKey:         fmt.Sprintf("%d", i),
				pulsarutils.SchedulerNameKey: pulsarutils.PulsarScheduler,
			},
			Payload: bytes,
		}
		// use a synchronous send here as the logic is simpler.
		// We send relatively few position markers so the performance penalty shouldn't be meaningful
		_, err = p.producer.Send(ctx, msg)
		if err != nil {
			return 0, err
		}
	}
	return uint32(p.numPartitions), nil
}

// createMessageRouter returns a custom Pulsar message router that routes the message to the partition given by the
// explicitPartitionKey msg property. If this property isn't present then it will fall back to the default Pulsar
// message routing logic
func createMessageRouter(options pulsar.ProducerOptions) func(*pulsar.ProducerMessage, pulsar.TopicMetadata) int {
	defaultRouter := pulsar.NewDefaultRouter(
		JavaStringHash,
		options.BatchingMaxMessages,
		options.BatchingMaxSize,
		options.BatchingMaxPublishDelay,
		options.DisableBatching)

	return func(msg *pulsar.ProducerMessage, md pulsar.TopicMetadata) int {
		explicitPartition, ok := msg.Properties[explicitPartitionKey]
		if ok {
			partition, err := strconv.ParseInt(explicitPartition, 10, 32)
			if err != nil {
				panic(errors.Errorf("cannot parse %s as int", explicitPartition))
			}
			if partition < 0 || uint32(partition) >= md.NumPartitions() {
				panic(errors.Errorf("requested partiton %d is not in the range 0-%d", partition, md.NumPartitions()-1))
			}
			return int(partition)
		}
		return defaultRouter(msg, md.NumPartitions())
	}
}

// JavaStringHash is the default hashing algorithm used by Pulsar
// copied from https://github.com/apache/pulsar-client-go/blob/master/pulsar/internal/hash.go
func JavaStringHash(s string) uint32 {
	var h uint32
	for i, size := 0, len(s); i < size; i++ {
		h = 31*h + uint32(s[i])
	}
	return h
}
