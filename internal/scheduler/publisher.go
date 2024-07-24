package scheduler

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/google/uuid"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	explicitPartitionKey = "armada_pulsar_partition"
)

// Publisher is an interface to be implemented by structs that handle publishing messages to pulsar
type Publisher interface {
	// PublishMessages will publish the supplied messages. A LeaderToken is provided and the
	// implementor may decide whether to publish based on the status of this token
	PublishMessages(ctx *armadacontext.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error

	// PublishMarkers publishes a single marker message for each Pulsar partition.  Each marker
	// massage contains the supplied group id, which allows all marker messages for a given call
	// to be identified.  The uint32 returned is the number of messages published
	PublishMarkers(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error)
}

// PulsarPublisher is the default implementation of Publisher
type PulsarPublisher struct {
	// Used to send events sequences to pulsar
	publisher pulsarutils.Publisher
	// Used to send position markers to pulsar
	producer pulsar.Producer
	// Number of partitions on the pulsar topic
	numPartitions int
}

func NewPulsarPublisher(
	pulsarClient pulsar.Client,
	producerOptions pulsar.ProducerOptions,
	maxEventsPerMessage int,
	maxAllowedMessageSize uint,
	sendTimeout time.Duration,
) (*PulsarPublisher, error) {
	id := uuid.NewString()
	producerOptions.Name = fmt.Sprintf("armada-scheduler-events-%s", id)
	publisher, err := pulsarutils.NewPulsarPublisher(pulsarClient, producerOptions, maxEventsPerMessage, maxAllowedMessageSize, sendTimeout)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	partitions, err := pulsarClient.TopicPartitions(producerOptions.Topic)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	producerOptions.Name = fmt.Sprintf("armada-scheduler-partitions-%s", id)
	producerOptions.MessageRouter = createMessageRouter(producerOptions)
	producer, err := pulsarClient.CreateProducer(producerOptions)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &PulsarPublisher{
		publisher:     publisher,
		producer:      producer,
		numPartitions: len(partitions),
	}, nil
}

// PublishMessages publishes all event sequences to pulsar if shouldPublish() returns true
func (p *PulsarPublisher) PublishMessages(ctx *armadacontext.Context, events []*armadaevents.EventSequence, shouldPublish func() bool) error {
	if shouldPublish() {
		return p.publisher.PublishMessages(ctx, events...)
	} else {
		return errors.New("Failed to publish as no longer leader")
	}
}

// PublishMarkers sends one pulsar message (containing an armadaevents.PartitionMarker) to each partition
// of the producer's Pulsar topic.
func (p *PulsarPublisher) PublishMarkers(ctx *armadacontext.Context, groupId uuid.UUID) (uint32, error) {
	for i := 0; i < p.numPartitions; i++ {
		pm := &armadaevents.PartitionMarker{
			GroupId:   armadaevents.ProtoUuidFromUuid(groupId),
			Partition: uint32(i),
		}
		es := &armadaevents.EventSequence{
			Queue:      "armada-scheduler",
			JobSetName: "armada-scheduler",
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: types.TimestampNow(),
					Event: &armadaevents.EventSequence_Event_PartitionMarker{
						PartitionMarker: pm,
					},
				},
			},
		}
		bytes, err := proto.Marshal(es)
		if err != nil {
			return 0, err
		}
		msg := &pulsar.ProducerMessage{
			Properties: map[string]string{
				explicitPartitionKey: fmt.Sprintf("%d", i),
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
