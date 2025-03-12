package ingest

import (
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	controlplaneevents_ingest_utils "github.com/armadaproject/armada/internal/common/ingest/controlplaneevents"
	"github.com/armadaproject/armada/internal/common/ingest/jobsetevents"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	f "github.com/armadaproject/armada/internal/common/ingest/testfixtures"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

const (
	jobId                   = "01f3j0g1md4qx7z5qb148qnh4r"
	runId                   = "123e4567-e89b-12d3-a456-426614174000"
	batchSize               = 3
	batchDuration           = 5 * time.Second
	jobSetEventsTopic       = "events"
	controlPlaneEventsTopic = "control-plane"
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
					RunId: runId,
					JobId: jobId,
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
					RunId:      runId,
					JobId:      jobId,
					ExecutorId: "k8sId1",
				},
			},
		},
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId: runId,
					JobId: jobId,
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
					RunId: runId,
					JobId: jobId,
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
					JobId: jobId,
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

type simpleEventSequenceConverter struct {
	t     *testing.T
	calls []*utils.EventsWithIds[*armadaevents.EventSequence]
}

func newSimpleEventSequenceConverter(t *testing.T) *simpleEventSequenceConverter {
	return &simpleEventSequenceConverter{
		t:     t,
		calls: []*utils.EventsWithIds[*armadaevents.EventSequence]{},
	}
}

func (s *simpleEventSequenceConverter) Convert(_ *armadacontext.Context, msg *utils.EventsWithIds[*armadaevents.EventSequence]) *simpleMessages {
	s.t.Helper()
	s.calls = append(s.calls, msg)
	assert.Len(s.t, msg.Events, len(msg.MessageIds))
	var converted []*simpleMessage
	for i, sequence := range msg.Events {
		converted = append(converted, &simpleMessage{
			id:   msg.MessageIds[i],
			size: proto.Size(sequence),
		})
	}
	return &simpleMessages{
		msgs: converted,
	}
}

type simpleControlPlaneEventConverter struct {
	t     *testing.T
	calls []*utils.EventsWithIds[*controlplaneevents.Event]
}

func newSimpleControlPlaneEventConverter(t *testing.T) *simpleControlPlaneEventConverter {
	return &simpleControlPlaneEventConverter{
		t:     t,
		calls: []*utils.EventsWithIds[*controlplaneevents.Event]{},
	}
}

func (s *simpleControlPlaneEventConverter) Convert(_ *armadacontext.Context, msg *utils.EventsWithIds[*controlplaneevents.Event]) *simpleMessages {
	s.t.Helper()
	s.calls = append(s.calls, msg)
	assert.Len(s.t, msg.Events, len(msg.MessageIds))
	var converted []*simpleMessage
	for i, sequence := range msg.Events {
		converted = append(converted, &simpleMessage{
			id:   msg.MessageIds[i],
			size: proto.Size(sequence),
		})
	}
	return &simpleMessages{
		msgs: converted,
	}
}

func TestRun_ControlPlaneEvents_HappyPath_SingleMessage(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, f.UpsertExecutorSettingsCordon)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleControlPlaneEventConverter(t)
	sink := newSimpleSink(t)

	pipeline := testControlPlaneEventsPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
	sink.assertProcessedMessageCount(len(messages))
}

func TestRun_ControlPlaneEvents_HappyPath_MultipleMessages(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, f.UpsertExecutorSettingsCordon)),
		pulsarutils.NewPulsarMessage(2, baseTime.Add(1*time.Second), marshal(t, f.UpsertExecutorSettingsUncordon)),
		pulsarutils.NewPulsarMessage(3, baseTime.Add(2*time.Second), marshal(t, f.DeleteExecutorSettings)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleControlPlaneEventConverter(t)
	sink := newSimpleSink(t)

	pipeline := testControlPlaneEventsPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
	sink.assertProcessedMessageCount(len(messages))
}

func TestRun_ControlPlaneEvents_LimitsProcessingBatchSize(t *testing.T) {
	tests := map[string]struct {
		numberOfMessages                 int
		batchSize                        int
		expectedNumberOfBatchesProcessed int
	}{
		"limits number of events processed per batch": {
			numberOfMessages:                 5,
			batchSize:                        2,
			expectedNumberOfBatchesProcessed: 3,
		},
		"limit can be exceeded by one message": {
			numberOfMessages: 6,
			batchSize:        5,
			// Batches should get limited to 2 messages, each containing 4 events
			expectedNumberOfBatchesProcessed: 2,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
			messages := []pulsar.Message{}
			for i := 0; i < tc.numberOfMessages; i++ {
				messages = append(messages, pulsarutils.NewPulsarMessage(i, baseTime, marshal(t, f.UpsertExecutorSettingsCordon)))
			}
			mockConsumer := newMockPulsarConsumer(t, messages, cancel)
			converter := newSimpleControlPlaneEventConverter(t)
			sink := newSimpleSink(t)

			pipeline := testControlPlaneEventsPipeline(mockConsumer, converter, sink)
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
				eventCount := len(call.Events)
				// BatchSize can be exceeded by one message
				assert.True(t, eventCount < tc.batchSize+1)
			}
			sink.assertDidProcess(messages)
			sink.assertProcessedMessageCount(len(messages))
		})
	}
}

func testControlPlaneEventsPipeline(consumer pulsar.Consumer, converter InstructionConverter[*simpleMessages, *controlplaneevents.Event], sink Sink[*simpleMessages]) *IngestionPipeline[*simpleMessages, *controlplaneevents.Event] {
	return &IngestionPipeline[*simpleMessages, *controlplaneevents.Event]{
		pulsarConfig: commonconfig.PulsarConfig{
			BackoffTime: time.Second,
		},
		pulsarTopic:            controlPlaneEventsTopic,
		pulsarSubscriptionName: "subscription",
		pulsarBatchDuration:    batchDuration,
		pulsarBatchSize:        batchSize,
		eventCounter:           controlplaneevents_ingest_utils.EventCounter,
		messageConverter:       controlplaneevents_ingest_utils.MessageUnmarshaller,
		batchMerger:            controlplaneevents_ingest_utils.BatchMerger,
		metricPublisher:        controlplaneevents_ingest_utils.BatchMetricPublisher,
		converter:              converter,
		sink:                   sink,
		metrics:                testMetrics,
		consumer:               consumer,
	}
}

type simpleSink struct {
	simpleMessages map[pulsar.MessageID]*simpleMessage
	t              *testing.T
	mutex          sync.Mutex
}

func newSimpleSink(t *testing.T) *simpleSink {
	return &simpleSink{
		simpleMessages: make(map[pulsar.MessageID]*simpleMessage),
		t:              t,
		mutex:          sync.Mutex{},
	}
}

func (s *simpleSink) Store(_ *armadacontext.Context, msg *simpleMessages) error {
	for _, simpleMessage := range msg.msgs {
		s.mutex.Lock()
		if simpleMessage != nil {
			s.simpleMessages[simpleMessage.id] = simpleMessage
		}
		s.mutex.Unlock()
	}
	return nil
}

func (s *simpleSink) assertDidProcess(messages []pulsar.Message) {
	s.t.Helper()
	for _, msg := range messages {
		simpleMessage, ok := s.simpleMessages[msg.ID()]
		assert.True(s.t, ok)
		assert.Greater(s.t, simpleMessage.size, 0)
	}
}

func (s *simpleSink) assertProcessedMessageCount(count int) {
	s.t.Helper()
	assert.Len(s.t, s.simpleMessages, count)
}

func TestRun_JobSetEvents_HappyPath_SingleMessage(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleEventSequenceConverter(t)
	sink := newSimpleSink(t)

	pipeline := testJobSetEventsPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
	sink.assertProcessedMessageCount(len(messages))
}

func TestRun_JobSetEvents_HappyPath_MultipleMessages(t *testing.T) {
	ctx, cancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	messages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
		pulsarutils.NewPulsarMessage(2, baseTime.Add(1*time.Second), marshal(t, pendingAndRunning)),
		pulsarutils.NewPulsarMessage(3, baseTime.Add(2*time.Second), marshal(t, failed)),
	}
	mockConsumer := newMockPulsarConsumer(t, messages, cancel)
	converter := newSimpleEventSequenceConverter(t)
	sink := newSimpleSink(t)

	pipeline := testJobSetEventsPipeline(mockConsumer, converter, sink)

	start := time.Now()
	err := pipeline.Run(ctx)
	assert.NoError(t, err)
	elapsed := time.Since(start)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockConsumer.assertDidAck(messages)
	sink.assertDidProcess(messages)
	sink.assertProcessedMessageCount(len(messages))
}

func TestRun_JobSetEvents_LimitsProcessingBatchSize(t *testing.T) {
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
			converter := newSimpleEventSequenceConverter(t)
			sink := newSimpleSink(t)

			pipeline := testJobSetEventsPipeline(mockConsumer, converter, sink)
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
				for _, seq := range call.Events {
					eventCount += len(seq.Events)
				}
				// BatchSize can be exceeded by one message, so at most the number of events in a single message
				assert.True(t, eventCount < tc.batchSize+tc.numberOfEventsPerMessage)
			}
			sink.assertDidProcess(messages)
			sink.assertProcessedMessageCount(len(messages))
		})
	}
}

// This will become a more common use case - multiple ingesters ingesting into the same sink
func TestRun_MultipleSimultaneousIngesters(t *testing.T) {
	jsCtx, jsCancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	cpCtx, cpCancel := armadacontext.WithDeadline(armadacontext.Background(), time.Now().Add(10*time.Second))
	jobSetMessages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(1, baseTime, marshal(t, succeeded)),
		pulsarutils.NewPulsarMessage(2, baseTime.Add(1*time.Second), marshal(t, pendingAndRunning)),
		pulsarutils.NewPulsarMessage(3, baseTime.Add(2*time.Second), marshal(t, failed)),
	}
	controlPlaneMessages := []pulsar.Message{
		pulsarutils.NewPulsarMessage(4, baseTime, marshal(t, f.UpsertExecutorSettingsCordon)),
		pulsarutils.NewPulsarMessage(5, baseTime.Add(1*time.Second), marshal(t, f.UpsertExecutorSettingsUncordon)),
		pulsarutils.NewPulsarMessage(6, baseTime.Add(2*time.Second), marshal(t, f.DeleteExecutorSettings)),
	}
	mockJobSetEventsConsumer := newMockPulsarConsumer(t, jobSetMessages, jsCancel)
	mockControlPlaneEventsConsumer := newMockPulsarConsumer(t, controlPlaneMessages, cpCancel)

	jobSetEventsConverter := newSimpleEventSequenceConverter(t)
	controlPlaneEventsConverter := newSimpleControlPlaneEventConverter(t)

	sink := newSimpleSink(t)

	jobSetEventsPipeline := testJobSetEventsPipeline(mockJobSetEventsConsumer, jobSetEventsConverter, sink)
	controlPlaneEventsPipeline := testControlPlaneEventsPipeline(mockControlPlaneEventsConsumer, controlPlaneEventsConverter, sink)

	var jsErr error
	var cpErr error
	wg := sync.WaitGroup{}
	start := time.Now()

	wg.Add(1)
	go func() {
		defer wg.Done()
		jsErr = jobSetEventsPipeline.Run(jsCtx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		cpErr = controlPlaneEventsPipeline.Run(cpCtx)
	}()

	wg.Wait()
	elapsed := time.Since(start)

	assert.NoError(t, jsErr)
	assert.NoError(t, cpErr)
	assert.LessOrEqual(t, elapsed, batchDuration*2)

	mockJobSetEventsConsumer.assertDidAck(jobSetMessages)
	mockControlPlaneEventsConsumer.assertDidAck(controlPlaneMessages)
	sink.assertDidProcess(jobSetMessages)
	sink.assertDidProcess(controlPlaneMessages)
	sink.assertProcessedMessageCount(len(controlPlaneMessages) + len(jobSetMessages))
}

func testJobSetEventsPipeline(consumer pulsar.Consumer, converter InstructionConverter[*simpleMessages, *armadaevents.EventSequence], sink Sink[*simpleMessages]) *IngestionPipeline[*simpleMessages, *armadaevents.EventSequence] {
	return &IngestionPipeline[*simpleMessages, *armadaevents.EventSequence]{
		pulsarConfig: commonconfig.PulsarConfig{
			BackoffTime: time.Second,
		},
		pulsarTopic:            jobSetEventsTopic,
		pulsarSubscriptionName: "subscription",
		pulsarBatchDuration:    batchDuration,
		pulsarBatchSize:        batchSize,
		eventCounter:           jobsetevents.EventCounter,
		messageConverter:       jobsetevents.MessageUnmarshaller,
		batchMerger:            jobsetevents.BatchMerger,
		metricPublisher:        jobsetevents.BatchMetricPublisher,
		converter:              converter,
		sink:                   sink,
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
					RunId: runId,
					JobId: jobId,
				},
			},
		})
	}
	return sequence
}

func marshal(t *testing.T, es proto.Message) []byte {
	payload, err := proto.Marshal(es)
	assert.NoError(t, err)
	return payload
}
