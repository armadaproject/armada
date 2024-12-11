package pulsarutils

import (
	"fmt"
	"go.uber.org/mock/gomock"
	"math"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/mocks"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	pscontrolplaneevents "github.com/armadaproject/armada/internal/common/pulsarutils/controlplaneevents"
	"github.com/armadaproject/armada/internal/common/pulsarutils/jobsetevents"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

const (
	topic                        = "testTopic"
	sendTimeout                  = time.Millisecond * 100
	defaultMaxEventsPerMessage   = 1000
	defaultMaxAllowedMessageSize = 1024 * 1024

	defaultExecutorName         = "testExecutor"
	defaultExecutorCordonReason = "bad executor"

	executorSettingsUpsertPulsarMessageKey = "executor-settings-upsert"
	executorSettingsDeletePulsarMessageKey = "executor-settings-delete"
)

var (
	DefaultTime      = time.Now().UTC()
	DefaultTimeProto = protoutil.ToTimestamp(DefaultTime)
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

			publisher, mockPulsarProducer := setUpJobSetEventsPublisherTest(t, tc.maxEventsPerMessage, tc.maxAllowedMessageSize)
			numberOfMessagesPublished := 0
			numberOfEventsPublished := 0
			var capturedEvents []*armadaevents.EventSequence
			expectedCounts := countEventSequences(tc.eventSequences)

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
			capturedCounts := countEventSequences(capturedEvents)
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

			publisher, mockPulsarProducer := setUpJobSetEventsPublisherTest(t, defaultMaxEventsPerMessage, defaultMaxAllowedMessageSize)
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

func TestPublishControlPlaneMessages(t *testing.T) {
	tests := map[string]struct {
		events                   []*controlplaneevents.Event
		expectedNumberOfMessages int
		expectedNumberOfEvents   int
		errorOnPublish           bool
	}{
		"singleEventPublish": {
			events: []*controlplaneevents.Event{
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
						ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
							Name:         defaultExecutorName,
							Cordoned:     true,
							CordonReason: defaultExecutorCordonReason,
						},
					},
				},
			},
			expectedNumberOfMessages: 1,
			expectedNumberOfEvents:   1,
			errorOnPublish:           false,
		},
		"multipleEvents": {
			events: []*controlplaneevents.Event{
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
						ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
							Name:         defaultExecutorName,
							Cordoned:     true,
							CordonReason: defaultExecutorCordonReason,
						},
					},
				},
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.Event_ExecutorSettingsUpsert{
						ExecutorSettingsUpsert: &controlplaneevents.ExecutorSettingsUpsert{
							Name:         defaultExecutorName,
							Cordoned:     false,
							CordonReason: "",
						},
					},
				},
			},
			expectedNumberOfMessages: 2,
			expectedNumberOfEvents:   2,
			errorOnPublish:           false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			publisher, mockPulsarProducer := setUpControlPlaneEventsPublisherTest(t)
			numberOfMessagesPublished := 0
			numberOfEventsPublished := 0
			var capturedEvents []*controlplaneevents.Event
			expectedCounts, err := countControlPlaneEvents(tc.events)
			assert.NoError(t, err)

			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx *armadacontext.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					numberOfMessagesPublished++
					es := &controlplaneevents.Event{}
					err := proto.Unmarshal(msg.Payload, es)
					require.NoError(t, err)
					capturedEvents = append(capturedEvents, es)
					numberOfEventsPublished += 1
					callback(NewMessageId(numberOfMessagesPublished), msg, nil)
				}).AnyTimes()

			err = publisher.PublishMessages(ctx, tc.events...)

			if tc.errorOnPublish {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				capturedCounts, err := countControlPlaneEvents(capturedEvents)
				assert.NoError(t, err)
				assert.Equal(t, expectedCounts, capturedCounts)
				assert.Equal(t, tc.expectedNumberOfMessages, numberOfMessagesPublished)
				assert.Equal(t, tc.expectedNumberOfEvents, numberOfEventsPublished)
			}
		})
	}
}

func setUpControlPlaneEventsPublisherTest(t *testing.T) (*PulsarPublisher[*controlplaneevents.Event], *mocks.MockProducer) {
	ctrl := gomock.NewController(t)
	mockPulsarClient := mocks.NewMockClient(ctrl)
	mockPulsarProducer := mocks.NewMockProducer(ctrl)
	mockPulsarClient.EXPECT().CreateProducer(gomock.Any()).Return(mockPulsarProducer, nil).Times(1)

	options := pulsar.ProducerOptions{Topic: topic}
	publisher, err := NewPulsarPublisher[*controlplaneevents.Event](
		mockPulsarClient,
		options,
		pscontrolplaneevents.PreProcess[*controlplaneevents.Event],
		pscontrolplaneevents.RetrieveKey[*controlplaneevents.Event],
		sendTimeout,
	)
	require.NoError(t, err)

	return publisher, mockPulsarProducer
}

func messageKeyFromEvent(event *controlplaneevents.Event) (string, error) {
	switch ev := event.Event.(type) {
	case *controlplaneevents.Event_ExecutorSettingsUpsert:
		return executorSettingsUpsertPulsarMessageKey, nil
	case *controlplaneevents.Event_ExecutorSettingsDelete:
		return executorSettingsDeletePulsarMessageKey, nil
	default:
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "controlplaneevent",
			Value:   ev,
			Message: fmt.Sprintf("unknown event type %T", ev),
		})
	}
}

func countControlPlaneEvents(events []*controlplaneevents.Event) (map[string]int, error) {
	countsById := make(map[string]int)
	for _, event := range events {
		eventType, err := messageKeyFromEvent(event)
		if err != nil {
			return nil, err
		}
		countsById[eventType] += 1
	}
	return countsById, nil
}

func setUpJobSetEventsPublisherTest(t *testing.T, maxEventsPerMessage int, maxAllowedMessageSize uint) (*PulsarPublisher[*armadaevents.EventSequence], *mocks.MockProducer) {
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
	preProcessor := jobsetevents.NewPreProcessor(maxEventsPerMessage, maxAllowedMessageSize)
	publisher, err := NewPulsarPublisher[*armadaevents.EventSequence](
		mockPulsarClient,
		options,
		preProcessor,
		jobsetevents.RetrieveKey,
		sendTimeout,
	)
	require.NoError(t, err)

	return publisher, mockPulsarProducer
}

func countEventSequences(es []*armadaevents.EventSequence) map[string]int {
	countsById := make(map[string]int)
	for _, sequence := range es {
		jobset := sequence.JobSetName
		count := countsById[jobset]
		count += len(sequence.Events)
		countsById[jobset] = count
	}
	return countsById
}
