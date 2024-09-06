package controlplaneevents

import (
	"strings"
	"testing"
	"time"

	"github.com/armadaproject/armada/internal/common/eventutil"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/controlplaneevents"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/mocks"
)

const (
	topic                        = "testTopic"
	sendTimeout                  = time.Millisecond * 100
	defaultMaxAllowedMessageSize = 1024 * 1024

	defaultExecutorName           = "testExecutor"
	defaultExecutorCordonReason   = "bad executor"
	defaultExecutorUncordonReason = "good executor"
)

var (
	DefaultTime              = time.Now().UTC()
	DefaultTimeProto         = protoutil.ToTimestamp(DefaultTime)
	executorCordonReasonLong = strings.Repeat(".", defaultMaxAllowedMessageSize)
)

func TestPublishMessages(t *testing.T) {
	tests := map[string]struct {
		events                   []*controlplaneevents.ControlPlaneEventV1
		expectedNumberOfMessages int
		expectedNumberOfEvents   int
		maxAllowedMessageSize    uint
		errorOnPublish           bool
	}{
		"withinAllowedMessageSize": {
			events: []*controlplaneevents.ControlPlaneEventV1{
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.ControlPlaneEventV1_CordonExecutor{
						CordonExecutor: &controlplaneevents.CordonExecutor{
							Name:   defaultExecutorName,
							Reason: defaultExecutorCordonReason,
						},
					},
				},
			},
			// Set this very low, expect it to spit every event into its own message
			maxAllowedMessageSize:    160,
			expectedNumberOfMessages: 1,
			expectedNumberOfEvents:   1,
			errorOnPublish:           false,
		},
		"maxAllowedMessageSize": {
			events: []*controlplaneevents.ControlPlaneEventV1{
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.ControlPlaneEventV1_CordonExecutor{
						CordonExecutor: &controlplaneevents.CordonExecutor{
							Name:   defaultExecutorName,
							Reason: executorCordonReasonLong,
						},
					},
				},
			},
			// Set this very low, expect it to spit every event into its own message
			maxAllowedMessageSize:    160,
			expectedNumberOfMessages: 1,
			expectedNumberOfEvents:   1,
			// Message larger than maximum allowed size, so we will fail validation
			errorOnPublish: true,
		},
		"multipleEvents": {
			events: []*controlplaneevents.ControlPlaneEventV1{
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.ControlPlaneEventV1_CordonExecutor{
						CordonExecutor: &controlplaneevents.CordonExecutor{
							Name:   defaultExecutorName,
							Reason: defaultExecutorCordonReason,
						},
					},
				},
				{
					Created: DefaultTimeProto,
					Event: &controlplaneevents.ControlPlaneEventV1_UncordonExecutor{
						UncordonExecutor: &controlplaneevents.UncordonExecutor{
							Name:   defaultExecutorName,
							Reason: defaultExecutorUncordonReason,
						},
					},
				},
			},
			// Set this very low, expect it to spit every event into its own message
			maxAllowedMessageSize:    160,
			expectedNumberOfMessages: 2,
			expectedNumberOfEvents:   2,
			errorOnPublish:           false,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithTimeout(armadacontext.Background(), 5*time.Second)
			defer cancel()

			publisher, mockPulsarProducer := setUpPublisherTest(t, tc.maxAllowedMessageSize)
			numberOfMessagesPublished := 0
			numberOfEventsPublished := 0
			var capturedEvents []*controlplaneevents.ControlPlaneEventV1
			expectedCounts, err := countEvents(tc.events)
			assert.NoError(t, err)

			mockPulsarProducer.
				EXPECT().
				SendAsync(gomock.Any(), gomock.Any(), gomock.Any()).
				DoAndReturn(func(ctx *armadacontext.Context, msg *pulsar.ProducerMessage, callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)) {
					numberOfMessagesPublished++
					es := &controlplaneevents.ControlPlaneEventV1{}
					err := proto.Unmarshal(msg.Payload, es)
					require.NoError(t, err)
					capturedEvents = append(capturedEvents, es)
					numberOfEventsPublished += 1
					callback(pulsarutils.NewMessageId(numberOfMessagesPublished), msg, nil)
				}).AnyTimes()

			err = publisher.PublishMessages(ctx, tc.events...)

			if tc.errorOnPublish {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				capturedCounts, err := countEvents(capturedEvents)
				assert.NoError(t, err)
				assert.Equal(t, expectedCounts, capturedCounts)
				assert.Equal(t, tc.expectedNumberOfMessages, numberOfMessagesPublished)
				assert.Equal(t, tc.expectedNumberOfEvents, numberOfEventsPublished)
			}
		})
	}
}

func setUpPublisherTest(t *testing.T, maxAllowedMessageSize uint) (*PulsarPublisher, *mocks.MockProducer) {
	ctrl := gomock.NewController(t)
	mockPulsarClient := mocks.NewMockClient(ctrl)
	mockPulsarProducer := mocks.NewMockProducer(ctrl)
	mockPulsarClient.EXPECT().CreateProducer(gomock.Any()).Return(mockPulsarProducer, nil).Times(1)

	if maxAllowedMessageSize == 0 {
		maxAllowedMessageSize = defaultMaxAllowedMessageSize
	}

	options := pulsar.ProducerOptions{Topic: topic}
	publisher, err := NewPulsarPublisher(mockPulsarClient, options, maxAllowedMessageSize, sendTimeout)
	require.NoError(t, err)

	return publisher, mockPulsarProducer
}

func countEvents(events []*controlplaneevents.ControlPlaneEventV1) (map[string]int, error) {
	countsById := make(map[string]int)
	for _, event := range events {
		eventType, err := eventutil.MessageKeyFromControlPlaneEvent(event)
		if err != nil {
			return nil, err
		}
		countsById[eventType] += 1
	}
	return countsById, nil
}
