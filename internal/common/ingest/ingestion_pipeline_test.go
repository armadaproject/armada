package ingest

import (
	"context"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/ingest/metrics"
	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/G-Research/armada/pkg/armadaevents"
)

const (
	jobIdString = "01f3j0g1md4qx7z5qb148qnh4r"
	runIdString = "123e4567-e89b-12d3-a456-426614174000"
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
	pulsar.Consumer
}

func newMockPulsarConsumer(messages []pulsar.Message, cancelFn func()) *mockPulsarConsumer {
	return &mockPulsarConsumer{
		messages: messages,
		acked:    make(map[pulsar.MessageID]bool),
		cancelFn: cancelFn,
	}
}

func (p *mockPulsarConsumer) Receive(ctx context.Context) (pulsar.Message, error) {
	if p.messageIdx < len(p.messages) {
		msg := p.messages[p.messageIdx]
		p.messageIdx++
		return msg, nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil, context.DeadlineExceeded
		}
	}
}

func (p *mockPulsarConsumer) AckID(messageId pulsar.MessageID) {
	p.acked[messageId] = true
	p.received++
	if p.received >= len(p.messages) {
		p.cancelFn()
	}
}

func (p *mockPulsarConsumer) didAck(messages []pulsar.Message) bool {
	if len(messages) != len(p.acked) {
		return false
	}
	for _, msg := range messages {
		_, ok := p.acked[msg.ID()]
		if !ok {
			return false
		}
	}
	return true
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

type simpleConverter struct{}

func newSimpleConverter() InstructionConverter[*simpleMessages] {
	return &simpleConverter{}
}

func (s *simpleConverter) Convert(ctx context.Context, msg *EventSequencesWithIds) *simpleMessages {
	if len(msg.EventSequences) != len(msg.MessageIds) {
		panic("non matching number of event sequences to message ids")
	}
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
}

func newSimpleSink() *simpleSink {
	return &simpleSink{
		simpleMessages: make(map[pulsar.MessageID]*simpleMessage),
	}
}

func (s *simpleSink) Store(ctx context.Context, msg *simpleMessages) error {
	for _, simpleMessage := range msg.msgs {
		s.simpleMessages[simpleMessage.id] = simpleMessage
	}
	return nil
}

func (s *simpleSink) didProcess(messages []pulsar.Message) bool {
	if len(s.simpleMessages) != len(messages) {
		return false
	}
	for _, msg := range messages {
		simpleMessage, ok := s.simpleMessages[msg.ID()]
		if !ok || simpleMessage.size == 0 {
			return false
		}
	}
	return true
}

func TestRun_HappyPath_SingleMessage(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
	}
	mockConsumer := newMockPulsarConsumer(messages, cancel)
	converter := newSimpleConverter()
	sink := newSimpleSink()

	pipeline := testPipeline(mockConsumer, converter, sink)

	err := pipeline.Run(ctx)
	assert.NoError(t, err)

	assert.True(t, mockConsumer.didAck(messages))
	assert.True(t, sink.didProcess(messages))
}

func TestRun_HappyPath_MultipleMessages(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
		pulsarutils.NewPulsarMessage(2, baseTime.Add(1*time.Second), marshal(t, pendingAndRunning)),
		pulsarutils.NewPulsarMessage(3, baseTime.Add(2*time.Second), marshal(t, failed)),
	}
	mockConsumer := newMockPulsarConsumer(messages, cancel)
	converter := newSimpleConverter()
	sink := newSimpleSink()

	pipeline := testPipeline(mockConsumer, converter, sink)

	err := pipeline.Run(ctx)
	assert.NoError(t, err)

	assert.True(t, mockConsumer.didAck(messages))
	assert.True(t, sink.didProcess(messages))
}

func testPipeline(consumer pulsar.Consumer, converter InstructionConverter[*simpleMessages], sink Sink[*simpleMessages]) *IngestionPipeline[*simpleMessages] {
	return NewIngestionPipeline[*simpleMessages](
		configuration.PulsarConfig{
			ReceiveTimeout: 10 * time.Second,
			BackoffTime:    time.Second,
		},
		"subscription",
		10,
		1*time.Second,
		converter,
		sink,
		configuration.MetricsConfig{},
		testMetrics,
		consumer,
	)
}

func marshal(t *testing.T, eventSequence *armadaevents.EventSequence) []byte {
	payload, err := proto.Marshal(succeeded)
	assert.NoError(t, err)
	return payload
}
