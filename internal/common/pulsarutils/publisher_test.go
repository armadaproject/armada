package pulsarutils

import (
	"math"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/mocks"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	topic                        = "testTopic"
	sendTimeout                  = time.Millisecond * 100
	defaultMaxEventsPerMessage   = 1000
	defaultMaxAllowedMessageSize = 1024 * 1024
)

func TestPublishMessages(t *testing.T) {
	tests := map[string]struct {
		eventSequences           []*armadaevents.EventSequence
		expectedNumberOfMessages int
		expectedNumberOfEvents   int
		maxEventsPerMessage      int
		maxAllowedMessageSize    uint
	}{
		"compacts events to reduce message count": {
			// same jobset, so should get compacted into a single message
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
				{
					JobSetName: "jobset2",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
			},
			expectedNumberOfMessages: 2,
			expectedNumberOfEvents:   4,
		},
		"maxEventsPerMessage": {
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}, {}, {}},
				},
				{
					JobSetName: "jobset2",
					Events:     []*armadaevents.EventSequence_Event{{}},
				},
			},
			maxEventsPerMessage:      2,
			expectedNumberOfMessages: 3,
			expectedNumberOfEvents:   5,
		},
		"maxAllowedMessageSize": {
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events: []*armadaevents.EventSequence_Event{
						{
							Event: &armadaevents.EventSequence_Event_SubmitJob{
								SubmitJob: &armadaevents.SubmitJob{
									DeduplicationId: "1000000000000000000000000000000000000",
								},
							},
						},
						{
							Event: &armadaevents.EventSequence_Event_SubmitJob{
								SubmitJob: &armadaevents.SubmitJob{
									DeduplicationId: "2000000000000000000000000000000000000",
								},
							},
						},
					},
				},
			},
			// Set this very low, expect it to spit every event into its own message
			maxAllowedMessageSize:    160,
			expectedNumberOfMessages: 2,
			expectedNumberOfEvents:   2,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			publisher, mockPulsarProducer := setUpPublisherTest(t, tc.maxEventsPerMessage, tc.maxAllowedMessageSize)
			numberOfMessagesPublished := 0
			numberOfEventsPublished := 0
			var capturedEvents []*armadaevents.EventSequence
			expectedCounts := countEvents(tc.eventSequences)

			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx *armadacontext.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					numberOfMessagesPublished++
					es := &armadaevents.EventSequence{}
					err := proto.Unmarshal(msg.Payload, es)
					require.NoError(t, err)
					capturedEvents = append(capturedEvents, es)
					numberOfEventsPublished += len(es.Events)
					callback(NewMessageId(numberOfMessagesPublished), msg, nil)
				}).AnyTimes()

			err := publisher.PublishMessages(ctx, tc.eventSequences...)

			assert.NoError(t, err)
			capturedCounts := countEvents(capturedEvents)
			assert.Equal(t, expectedCounts, capturedCounts)
			assert.Equal(t, tc.expectedNumberOfMessages, numberOfMessagesPublished)
			assert.Equal(t, tc.expectedNumberOfEvents, numberOfEventsPublished)
		})
	}
}

func TestPublishMessages_HandlesFailureModes(t *testing.T) {
	tests := map[string]struct {
		publishDuration          time.Duration
		numSuccessfulPublishes   int
		expectedNumberOfMessages int
		expectedNumberOfEvents   int
		eventSequences           []*armadaevents.EventSequence
	}{
		"Returns error if all eventSequences fail to publish": {
			numSuccessfulPublishes: 0,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
			},
			expectedNumberOfMessages: 0,
			expectedNumberOfEvents:   0,
		},
		"Returns error if some eventSequences fail to publish": {
			numSuccessfulPublishes: 1,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
				{
					JobSetName: "jobset2",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
			},
			expectedNumberOfMessages: 1,
			expectedNumberOfEvents:   2,
		},
		"Returns error if publishing exceeds send timeout": {
			numSuccessfulPublishes: math.MaxInt,
			publishDuration:        sendTimeout * 2,
			eventSequences: []*armadaevents.EventSequence{
				{
					JobSetName: "jobset1",
					Events:     []*armadaevents.EventSequence_Event{{}, {}},
				},
			},
			expectedNumberOfMessages: 0,
			expectedNumberOfEvents:   0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			publisher, mockPulsarProducer := setUpPublisherTest(t, defaultMaxEventsPerMessage, defaultMaxAllowedMessageSize)
			numberOfMessagesPublished := 0
			numberOfEventsPublished := 0

			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx *armadacontext.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					// A little bit shonky here, we're assuming the pulsar SendAsync actually returns an error on context expiry
					time.Sleep(tc.publishDuration)
					if ctx.Err() != nil {
						callback(nil, msg, ctx.Err())
						return
					}
					if numberOfMessagesPublished >= tc.numSuccessfulPublishes {
						callback(NewMessageId(numberOfMessagesPublished), msg, errors.New("error from mock pulsar producer"))
					} else {
						es := &armadaevents.EventSequence{}
						err := proto.Unmarshal(msg.Payload, es)
						require.NoError(t, err)

						numberOfMessagesPublished++
						numberOfEventsPublished += len(es.Events)
						callback(NewMessageId(numberOfMessagesPublished), msg, nil)
					}
				}).AnyTimes()

			err := publisher.PublishMessages(ctx, tc.eventSequences...)

			assert.Error(t, err)
			assert.Equal(t, tc.expectedNumberOfMessages, numberOfMessagesPublished)
			assert.Equal(t, tc.expectedNumberOfEvents, numberOfEventsPublished)
		})
	}
}

func setUpPublisherTest(t *testing.T, maxEventsPerMessage int, maxAllowedMessageSize uint) (*PulsarPublisher, *mocks.MockProducer) {
	ctrl := gomock.NewController(t)
	mockPulsarClient := mocks.NewMockClient(ctrl)
	mockPulsarProducer := mocks.NewMockProducer(ctrl)
	mockPulsarClient.EXPECT().CreateProducer(gomock.Any()).Return(mockPulsarProducer, nil).Times(1)

	if maxAllowedMessageSize == 0 {
		maxAllowedMessageSize = defaultMaxAllowedMessageSize
	}

	if maxEventsPerMessage == 0 {
		maxEventsPerMessage = defaultMaxEventsPerMessage
	}

	options := pulsar.ProducerOptions{Topic: topic}
	publisher, err := NewPulsarPublisher(mockPulsarClient, options, maxEventsPerMessage, maxAllowedMessageSize, sendTimeout)
	require.NoError(t, err)

	return publisher, mockPulsarProducer
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
