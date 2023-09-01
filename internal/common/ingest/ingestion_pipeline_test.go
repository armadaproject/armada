package ingest

import (
	gocontext "context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobIdString   = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString   = "123e4567-e89b-12d3-a456-426614174000"
	batchSize     = 3
	batchDuration = 5 * time.Second
)

var (
	jobIdProto, _ = armadaevents.ProtoUuidFromUlidString(jobIdString)
	runIdProto    = armadaevents.ProtoUuidFromUuid(uuid.MustParse(runIdString))
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	testMetrics   = metrics.NewMetrics("test")
)

var succeeded = &armadaevents.EventSequence{
	Queue:      "test",
	JobSetName: "test",
	UserId:     "chrisma",
	Events: []*armadaevents.EventSequence_Event{
		{
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId: runIdProto,
					JobId: jobIdProto,
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
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_JobRunLeased{
				JobRunLeased: &armadaevents.JobRunLeased{
					RunId:      runIdProto,
					JobId:      jobIdProto,
					ExecutorId: "k8sId1",
				},
			},
		},
		{
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId: runIdProto,
					JobId: jobIdProto,
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
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_JobRunErrors{
				JobRunErrors: &armadaevents.JobRunErrors{
					RunId: runIdProto,
					JobId: jobIdProto,
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
			Created: &baseTime,
			Event: &armadaevents.EventSequence_Event_JobErrors{
				JobErrors: &armadaevents.JobErrors{
					JobId: jobIdProto,
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
	messages   []pulsar.Message
	messageIdx int
	acked      map[pulsar.MessageID]bool
	received   int
	cancelFn   func()
	t          *testing.T
	pulsar.Consumer
}

func newMockPulsarConsumer(t *testing.T, messages []pulsar.Message, cancelFn func()) *mockPulsarConsumer {
	return &mockPulsarConsumer{
		messages: messages,
		acked:    make(map[pulsar.MessageID]bool),
		cancelFn: cancelFn,
		t:        t,
	}
}

func (p *mockPulsarConsumer) Receive(ctx gocontext.Context) (pulsar.Message, error) {
	if p.messageIdx < len(p.messages) {
		msg := p.messages[p.messageIdx]
		p.messageIdx++
		return msg, nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil, gocontext.DeadlineExceeded
		}
	}
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
	t *testing.T
}

func newSimpleConverter(t *testing.T) InstructionConverter[*simpleMessages] {
	return &simpleConverter{t}
}

func (s *simpleConverter) Convert(_ *context.ArmadaContext, msg *EventSequencesWithIds) *simpleMessages {
	s.t.Helper()
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

func (s *simpleSink) Store(_ *context.ArmadaContext, msg *simpleMessages) error {
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
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
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
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
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

func testPipeline(consumer pulsar.Consumer, converter InstructionConverter[*simpleMessages], sink Sink[*simpleMessages]) *IngestionPipeline[*simpleMessages] {
	return &IngestionPipeline[*simpleMessages]{
		pulsarConfig: configuration.PulsarConfig{
			ReceiveTimeout: 10 * time.Second,
			BackoffTime:    time.Second,
		},
		pulsarSubscriptionName: "subscription",
		pulsarBatchDuration:    batchDuration,
		pulsarBatchSize:        batchSize,
		converter:              converter,
		sink:                   sink,
		metricsConfig:          configuration.MetricsConfig{},
		metrics:                testMetrics,
		consumer:               consumer,
		msgFilter:              func(msg pulsar.Message) bool { return true },
	}
}

func marshal(t *testing.T, es *armadaevents.EventSequence) []byte {
	payload, err := proto.Marshal(es)
	assert.NoError(t, err)
	return payload
}
