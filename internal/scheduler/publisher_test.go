package scheduler

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/pulsarutils"
	schedulermocks "github.com/armadaproject/armada/internal/scheduler/mocks"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	topic         = "testTopic"
	numPartitions = 100
)

func TestPulsarPublisher_TestPublish(t *testing.T) {
	tests := map[string]struct {
		eventSequences         []*armadaevents.EventSequence
		numSuccessfulPublishes int
		amLeader               bool
		expectedError          bool
	}{
		"Publish if leader": {
			amLeader:               true,
			numSuccessfulPublishes: math.MaxInt,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
				{
					JobSetName: "jobset2",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
			},
		},
		"Don't publish if not leader": {
			amLeader:               false,
			numSuccessfulPublishes: math.MaxInt,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
			},
		},
		"Return error if all events fail to publish": {
			amLeader:               true,
			numSuccessfulPublishes: 0,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
			},
			expectedError: true,
		},
		"Return error if some events fail to publish": {
			amLeader:               true,
			numSuccessfulPublishes: 1,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
				{
					JobSetName: "jobset2",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
			},
			expectedError: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctrl := gomock.NewController(t)
			mockPulsarClient := schedulermocks.NewMockClient(ctrl)
			mockPulsarProducer := schedulermocks.NewMockProducer(ctrl)
			mockPulsarClient.EXPECT().CreateProducer(gomock.Any()).Return(mockPulsarProducer, nil).Times(1)
			mockPulsarClient.EXPECT().TopicPartitions(topic).Return(make([]string, numPartitions), nil)
			numPublished := 0
			var capturedEvents []*armadaevents.EventSequence
			expectedCounts := make(map[string]int)
			if tc.amLeader {
				expectedCounts = countEvents(tc.eventSequences)
			}

			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					es := &armadaevents.EventSequence{}
					err := proto.Unmarshal(msg.Payload, es)
					require.NoError(t, err)
					capturedEvents = append(capturedEvents, es)
					numPublished++
					if numPublished > tc.numSuccessfulPublishes {
						callback(pulsarutils.NewMessageId(numPublished), msg, errors.New("error from mock pulsar producer"))
					} else {
						callback(pulsarutils.NewMessageId(numPublished), msg, nil)
					}
				}).AnyTimes()

			options := pulsar.ProducerOptions{Topic: topic}
			publisher, err := NewPulsarPublisher(mockPulsarClient, options, 5*time.Second)
			require.NoError(t, err)
			err = publisher.PublishMessages(ctx, tc.eventSequences, func() bool { return tc.amLeader })

			// Check that we get an error if one is expected
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Check that we got the messages that we expect
			if tc.amLeader {
				capturedCounts := countEvents(capturedEvents)
				assert.Equal(t, expectedCounts, capturedCounts)
			}
		})
	}
}

func TestPulsarPublisher_TestPublishMarkers(t *testing.T) {
	allPartitions := make(map[string]bool, 0)
	for i := 0; i < numPartitions; i++ {
		allPartitions[fmt.Sprintf("%d", i)] = true
	}
	tests := map[string]struct {
		numSuccessfulPublishes int
		expectedError          bool
		expectedPartitions     map[string]bool
	}{
		"Publish successful": {
			numSuccessfulPublishes: math.MaxInt,
			expectedError:          false,
			expectedPartitions:     allPartitions,
		},
		"All Publishes fail": {
			numSuccessfulPublishes: 0,
			expectedError:          true,
		},
		"Some Publishes fail": {
			numSuccessfulPublishes: 10,
			expectedError:          true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mockPulsarClient := schedulermocks.NewMockClient(ctrl)
			mockPulsarProducer := schedulermocks.NewMockProducer(ctrl)
			mockPulsarClient.EXPECT().CreateProducer(gomock.Any()).Return(mockPulsarProducer, nil).Times(1)
			mockPulsarClient.EXPECT().TopicPartitions(topic).Return(make([]string, numPartitions), nil)
			numPublished := 0
			capturedPartitions := make(map[string]bool)

			mockPulsarProducer.
				EXPECT().
				Send(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, msg *pulsar.ProducerMessage) (pulsar.MessageID, error) {
					numPublished++
					key, ok := msg.Properties[explicitPartitionKey]
					if ok {
						capturedPartitions[key] = true
					}
					if numPublished > tc.numSuccessfulPublishes {
						return pulsarutils.NewMessageId(numPublished), errors.New("error from mock pulsar producer")
					}
					return pulsarutils.NewMessageId(numPublished), nil
				}).AnyTimes()

			options := pulsar.ProducerOptions{Topic: topic}
			ctx := context.TODO()
			publisher, err := NewPulsarPublisher(mockPulsarClient, options, 5*time.Second)
			require.NoError(t, err)

			published, err := publisher.PublishMarkers(ctx, uuid.New())

			// Check that we get an error if one is expected
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if !tc.expectedError {
				assert.Equal(t, uint32(numPartitions), published)
				assert.Equal(t, tc.expectedPartitions, capturedPartitions)
			}
		})
	}
}

type TopicMetadata struct{}

func (t TopicMetadata) NumPartitions() uint32 {
	return 20
}

func TestMessageRouter(t *testing.T) {
	options := pulsar.ProducerOptions{Topic: topic}
	router := createMessageRouter(options)

	t.Run("Route explicit partition", func(t *testing.T) {
		for i := 0; i < 20; i++ {
			msg := &pulsar.ProducerMessage{
				Properties: map[string]string{explicitPartitionKey: fmt.Sprintf("%d", i)},
			}
			assert.Equal(t, i, router(msg, TopicMetadata{}))
		}
	})

	t.Run("Route with key", func(t *testing.T) {
		keys := []string{"foo", "bar", "baz"}
		for _, key := range keys {
			msg := &pulsar.ProducerMessage{
				Key: key,
			}
			assert.Equal(t, int(JavaStringHash(key)%20), router(msg, TopicMetadata{}))
		}
	})
}

func TestJavaStringHash(t *testing.T) {
	javaHashValues := map[string]uint32{"": 0x0, "hello": 0x5e918d2, "test": 0x364492}
	for str, expectedHash := range javaHashValues {
		t.Run(str, func(t *testing.T) {
			assert.Equal(t, expectedHash, JavaStringHash(str))
		})
	}
}

func countEvents(es []*armadaevents.EventSequence) map[string]int {
	countsById := make(map[string]int)
	for _, sequence := range es {
		jobset := sequence.JobSetName
		count := countsById[jobset]
		count += len(sequence.Events)
		countsById[jobset] = count
	}
	return countsById
}
