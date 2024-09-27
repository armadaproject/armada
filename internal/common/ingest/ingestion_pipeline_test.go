package ingest

import (
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobId         = "01f3j0g1md4qx7z5qb148qnh4r"
	runId         = "123e4567-e89b-12d3-a456-426614174000"
	batchSize     = 3
	batchDuration = 5 * time.Second
)

var (
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeProto = protoutil.ToTimestamp(baseTime)
	testMetrics   = metrics.NewMetrics("test")
)

var succeeded = &armadaevents.EventSequence{
	Queue:      "test",
	JobSetName: "test",
	UserId:     "chrisma",
	Events: []*armadaevents.EventSequence_Event{
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunIdStr: runId,
					JobIdStr: jobId,
				},
			},
		},
	},
}

var pendingAndRunning = &armadaevents.EventSequence{
	Queue:      "test",
	JobSetName: "test",
	UserId:     "chrisma",
	Events: []*armadaevents.EventSequence_Event{
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunIdStr:   runId,
					JobIdStr:   jobId,
					ExecutorId: "k8sId1",
				},
			},
		},
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunIdStr: runId,
					JobIdStr: jobId,
				},
			},
		},
	},
}

var failed = &armadaevents.EventSequence{
	Queue:      "test",
	JobSetName: "test",
	UserId:     "chrisma",
	Events: []*armadaevents.EventSequence_Event{
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunIdStr: runId,
					JobIdStr: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason:   &armadaevents.Error_ExecutorError{},
						},
					},
				},
			},
		},
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobIdStr: jobId,
					Errors: []*armadaevents.Error{
						{
							Terminal: true,
							Reason:   &armadaevents.Error_ExecutorError{},
						},
					},
				},
			},
		},
	},
}

type mockPulsarConsumer struct {
	messages    []pulsar.Message
	receiveChan chan pulsar.ConsumerMessage
	acked       map[pulsar.MessageID]bool
	received    int
	cancelFn    func()
	t           *testing.T
	pulsar.Consumer
}

func newMockPulsarConsumer(t *testing.T, messages []pulsar.Message, cancelFn func()) *mockPulsarConsumer {
	receiveChan := make(chan pulsar.ConsumerMessage, len(messages))
	consumer := &mockPulsarConsumer{
		messages:    messages,
		acked:       make(map[pulsar.MessageID]bool),
		cancelFn:    cancelFn,
		t:           t,
		receiveChan: receiveChan,
	}
	for _, m := range messages {
		receiveChan <- pulsar.ConsumerMessage{
			Consumer: consumer,
			Message:  m,
		}
	}
	return consumer
}

func (p *mockPulsarConsumer) Chan() <-chan pulsar.ConsumerMessage {
	return p.receiveChan
}

func (p *mockPulsarConsumer) AckID(messageId pulsar.MessageID) error {
	p.acked[messageId] = true
	p.received++
	if p.received >= len(p.messages) {
		p.cancelFn()
	}
	return nil
}

func (p *mockPulsarConsumer) assertDidAck(messages []pulsar.Message) {
	p.t.Helper()
	assert.Len(p.t, p.acked, len(messages))
	for _, msg := range messages {
		_, ok := p.acked[msg.ID()]
		assert.True(p.t, ok)
	}
}

func (p *mockPulsarConsumer) Close() {
	// do nothing
}

type simpleMessage struct {
	id   pulsar.MessageID
	size int
}

type simpleMessages struct {
	msgs []*simpleMessage
}

func (s *simpleMessages) GetMessageIDs() []pulsar.MessageID {
	messageIds := make([]pulsar.MessageID, len(s.msgs))
	for i, msg := range s.msgs {
		messageIds[i] = msg.id
	}
	return messageIds
}

type simpleConverter struct {
	t     *testing.T
	calls []*EventSequencesWithIds
}

func newSimpleConverter(t *testing.T) *simpleConverter {
	return &simpleConverter{
		t:     t,
		calls: []*EventSequencesWithIds{},
	}
}

func (s *simpleConverter) Convert(_ *armadacontext.Context, msg *EventSequencesWithIds) *simpleMessages {
	s.t.Helper()
	s.calls = append(s.calls, msg)
	assert.Len(s.t, msg.EventSequences, len(msg.MessageIds))
	var converted []*simpleMessage
	for i, sequence := range msg.EventSequences {
		converted = append(converted, &simpleMessage{
			id:   msg.MessageIds[i],
			size: sequence.Size(),
		})
	}
	return &simpleMessages{
		msgs: converted,
	}
}

type simpleSink struct {
	simpleMessages map[pulsar.MessageID]*simpleMessage
	t              *testing.T
}

func newSimpleSink(t *testing.T) *simpleSink {
	return &simpleSink{
		simpleMessages: make(map[pulsar.MessageID]*simpleMessage),
		t:              t,
	}
}

func (s *simpleSink) Store(_ *armadacontext.Context, msg *simpleMessages) error {
	for _, simpleMessage := range msg.msgs {
		s.simpleMessages[simpleMessage.id] = simpleMessage
	}
	return nil
}

func (s *simpleSink) assertDidProcess(messages []pulsar.Message) {
	s.t.Helper()
	assert.Len(s.t, s.simpleMessages, len(messages))
	for _, msg := range messages {
		simpleMessage, ok := s.simpleMessages[msg.ID()]
		assert.True(s.t, ok)
		assert.Greater(s.t, simpleMessage.size, 0)
	}
}

func TestRun_HappyPath_SingleMessage(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleConverter(t)
	sink := newSimpleSink(t)

	pipeline := testPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
}

func TestRun_HappyPath_MultipleMessages(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
		pulsarutils.NewPulsarMessage(2, baseTime.Add(1*time.Second), marshal(t, pendingAndRunning)),
		pulsarutils.NewPulsarMessage(3, baseTime.Add(2*time.Second), marshal(t, failed)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleConverter(t)
	sink := newSimpleSink(t)

	pipeline := testPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
}

func TestRun_LimitsProcessingBatchSize(t *testing.T) {
	tests := map[string]struct {
		numberOfEventsPerMessage         int
		numberOfMessages                 int
		batchSize                        int
		expectedNumberOfBatchesProcessed int
	}{
		"limits number of events processed per batch": {
			numberOfEventsPerMessage:         1,
			numberOfMessages:                 5,
			batchSize:                        2,
			expectedNumberOfBatchesProcessed: 3,
		},
		"limit can be exceeded by one message": {
			numberOfEventsPerMessage: 4,
			numberOfMessages:         6,
			batchSize:                5,
			// Batches should get limited to 2 messages, each containing 4 events
			expectedNumberOfBatchesProcessed: 3,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
			messages := []pulsar.Message{}
			for i := 0; i < tc.numberOfMessages; i++ {
				messages = append(messages, pulsarutils.NewPulsarMessage(i, baseTime, marshal(t, generateEventSequence(tc.numberOfEventsPerMessage))))
			}
			mockConsumer := newMockPulsarConsumer(t, messages, cancel)
			converter := newSimpleConverter(t)
			sink := newSimpleSink(t)

			pipeline := testPipeline(mockConsumer, converter, sink)
			// Should limit batches of event sequences based on batch size
			pipeline.pulsarBatchSize = tc.batchSize

			start := time.Now()
			err := pipeline.Run(ctx)
			assert.NoError(t, err)
			elapsed := time.Since(start)
			expectedMaximumDuration := time.Duration(tc.expectedNumberOfBatchesProcessed) * batchDuration

			assert.LessOrEqual(t, elapsed, expectedMaximumDuration)
			mockConsumer.assertDidAck(messages)
			assert.Len(t, converter.calls, tc.expectedNumberOfBatchesProcessed)
			for _, call := range converter.calls {
				eventCount := 0
				for _, seq := range call.EventSequences {
					eventCount += len(seq.Events)
				}
				// BatchSize can be exceeded by one message, so at most the number of events in a single message
				assert.True(t, eventCount < tc.batchSize+tc.numberOfEventsPerMessage)
			}
			sink.assertDidProcess(messages)
		})
	}
}

func testPipeline(consumer pulsar.Consumer, converter InstructionConverter[*simpleMessages], sink Sink[*simpleMessages]) *IngestionPipeline[*simpleMessages] {
	return &IngestionPipeline[*simpleMessages]{
		pulsarConfig: commonconfig.PulsarConfig{
			BackoffTime: time.Second,
		},
		pulsarSubscriptionName: "subscription",
		pulsarBatchDuration:    batchDuration,
		pulsarBatchSize:        batchSize,
		converter:              converter,
		sink:                   sink,
		metricsPort:            8080,
		metrics:                testMetrics,
		consumer:               consumer,
	}
}

func generateEventSequence(numberOfEvents int) *armadaevents.EventSequence {
	sequence := &armadaevents.EventSequence{
		Queue:      "test",
		JobSetName: "test",
		UserId:     "chrisma",
		Events:     []*armadaevents.EventSequence_Event{},
	}
	for i := 0; i < numberOfEvents; i++ {
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunIdStr: runId,
					JobIdStr: jobId,
				},
			},
		})
	}
	return sequence
}

func marshal(t *testing.T, es *armadaevents.EventSequence) []byte {
	payload, err := proto.Marshal(es)
	assert.NoError(t, err)
	return payload
}
