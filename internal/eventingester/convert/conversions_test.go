package convert

import (
	"math/rand"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	eventingestermetrics "github.com/armadaproject/armada/internal/eventingester/metrics"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobset = "testJobset"
	queue  = "testQueue"
	JobId  = "01f3j0g1md4qx7z5qb148qnh4r"
	RunId  = "123e4567-e89b-12d3-a456-426614174000"
)

var (
	baseTime, _   = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeProto = protoutil.ToTimestamp(baseTime)
)

// Succeeded
var jobRunSucceeded = &armadaevents.EventSequence_Event{
	Created: baseTimeProto,
	Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
		JobRunSucceeded: &armadaevents.JobRunSucceeded{
			RunId: RunId,
			JobId: JobId,
		},
	},
}

// Cancelled
var cancelled = &armadaevents.EventSequence_Event{
	Created: baseTimeProto,
	Event: &armadaevents.EventSequence_Event_CancelledJob{
		CancelledJob: &armadaevents.CancelledJob{
			JobId: JobId,
		},
	},
}

func TestSingle(t *testing.T) {
	msg := NewMsg(jobRunSucceeded)
	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{jobRunSucceeded},
	}
	assert.Equal(t, msg.MessageIds, batchUpdate.MessageIds)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	assert.Equal(t, queue, event.Queue)
	assert.Equal(t, jobset, event.Jobset)
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

func TestMultiple(t *testing.T) {
	msg := NewMsg(cancelled, jobRunSucceeded)
	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	expectedSequence := armadaevents.EventSequence{
		Events: []*armadaevents.EventSequence_Event{cancelled, jobRunSucceeded},
	}
	assert.Equal(t, msg.MessageIds, batchUpdate.MessageIds)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	assert.Equal(t, queue, event.Queue)
	assert.Equal(t, jobset, event.Jobset)
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, expectedSequence.Events, es.Events)
}

// Cancellation reason should not be in event storage
func TestCancelled(t *testing.T) {
	msg := NewMsg(&armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_CancelJob{
			CancelJob: &armadaevents.CancelJob{
				JobId:  JobId,
				Reason: "some reason 1",
			},
		},
	}, &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_CancelJobSet{
			CancelJobSet: &armadaevents.CancelJobSet{
				Reason: "some reason 2",
			},
		},
	}, &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_CancelledJob{
			CancelledJob: &armadaevents.CancelledJob{
				JobId:  JobId,
				Reason: "some reason 3",
			},
		},
	})
	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	expectedEvents := []*armadaevents.EventSequence_Event{
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_CancelJob{
				CancelJob: &armadaevents.CancelJob{
					JobId: JobId,
				},
			},
		},
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_CancelJobSet{
				CancelJobSet: &armadaevents.CancelJobSet{},
			},
		},
		{
			Created: baseTimeProto,
			Event: &armadaevents.EventSequence_Event_CancelledJob{
				CancelledJob: &armadaevents.CancelledJob{
					JobId: JobId,
				},
			},
		},
	}
	assert.Equal(t, expectedEvents, es.Events)
}

func NewMsg(event ...*armadaevents.EventSequence_Event) *utils.EventsWithIds[*armadaevents.EventSequence] {
	seq := &armadaevents.EventSequence{
		Queue:      queue,
		JobSetName: jobset,
		Events:     event,
	}
	return &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events:     []*armadaevents.EventSequence{seq},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(rand.Int())},
	}
}

func simpleEventConverter() *EventConverter {
	compressor, _ := compress.NewZlibCompressor(0)
	return &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: 1024,
	}
}

func extractEventSeq(b []byte) (*armadaevents.EventSequence, error) {
	decompressor := compress.NewZlibDecompressor()
	decompressed, err := decompressor.Decompress(b)
	if err != nil {
		return nil, err
	}
	es := &armadaevents.EventSequence{}
	err = proto.Unmarshal(decompressed, es)
	if err != nil {
		return nil, err
	}
	return es, nil
}

func TestConvertMetrics_HappyPath(t *testing.T) {
	t.Cleanup(func() {
		prometheus.Unregister(eventingestermetrics.UncompressedEventSizeMetric())
		prometheus.Unregister(eventingestermetrics.EstimatedCompressedEventSizeMetric())
	})

	msg := NewMsg(jobRunSucceeded, cancelled)
	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), msg)

	assert.Equal(t, 1, len(batchUpdate.Events))
	event := batchUpdate.Events[0]
	es, err := extractEventSeq(event.Event)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(es.Events))

	uncompressedJobRunSucceeded := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queue,
		),
	)
	assert.Greater(t, uncompressedJobRunSucceeded, float64(0), "uncompressed size for JobRunSucceeded should be > 0")

	uncompressedCancelled := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queue,
		),
	)
	assert.Greater(t, uncompressedCancelled, float64(0), "uncompressed size for CancelledJob should be > 0")

	estimatedJobRunSucceeded := testutil.ToFloat64(
		eventingestermetrics.EstimatedCompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queue,
		),
	)
	assert.Greater(t, estimatedJobRunSucceeded, float64(0), "estimated compressed size for JobRunSucceeded should be > 0")

	estimatedCancelled := testutil.ToFloat64(
		eventingestermetrics.EstimatedCompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queue,
		),
	)
	assert.Greater(t, estimatedCancelled, float64(0), "estimated compressed size for CancelledJob should be > 0")

	totalUncompressed := uncompressedJobRunSucceeded + uncompressedCancelled
	totalEstimatedCompressed := estimatedJobRunSucceeded + estimatedCancelled
	batchRatio := totalEstimatedCompressed / totalUncompressed

	assert.Greater(t, batchRatio, float64(0), "batch ratio should be > 0")
}

func TestConvertMetrics_ZeroDenominator(t *testing.T) {
	t.Cleanup(func() {
		prometheus.Unregister(eventingestermetrics.UncompressedEventSizeMetric())
		prometheus.Unregister(eventingestermetrics.EstimatedCompressedEventSizeMetric())
	})

	emptyMsg := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      queue,
				JobSetName: jobset,
				Events:     []*armadaevents.EventSequence_Event{},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(rand.Int())},
	}

	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), emptyMsg)

	assert.Equal(t, 0, len(batchUpdate.Events))
}

func TestConvert_PopulatesBatchAggregateFields(t *testing.T) {
	t.Cleanup(func() {
		prometheus.Unregister(eventingestermetrics.UncompressedEventSizeMetric())
		prometheus.Unregister(eventingestermetrics.EstimatedCompressedEventSizeMetric())
	})

	queueA := "queue-a"
	queueB := "queue-b"

	msgA := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      queueA,
				JobSetName: jobset,
				Events:     []*armadaevents.EventSequence_Event{jobRunSucceeded},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}

	msgB := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      queueB,
				JobSetName: jobset,
				Events:     []*armadaevents.EventSequence_Event{cancelled},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(2)},
	}

	combined := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events:     append(msgA.Events, msgB.Events...),
		MessageIds: append(msgA.MessageIds, msgB.MessageIds...),
	}

	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), combined)

	assert.Equal(t, 2, len(batchUpdate.Events))

	assert.NotNil(t, batchUpdate.UncompressedTotalByQueue)
	assert.NotNil(t, batchUpdate.CompressedTotalByQueue)

	assert.Contains(t, batchUpdate.UncompressedTotalByQueue, queueA)
	assert.Contains(t, batchUpdate.UncompressedTotalByQueue, queueB)
	assert.Contains(t, batchUpdate.CompressedTotalByQueue, queueA)
	assert.Contains(t, batchUpdate.CompressedTotalByQueue, queueB)

	assert.Greater(t, batchUpdate.UncompressedTotalByQueue[queueA], uint64(0))
	assert.Greater(t, batchUpdate.UncompressedTotalByQueue[queueB], uint64(0))
	assert.Greater(t, batchUpdate.CompressedTotalByQueue[queueA], uint64(0))
	assert.Greater(t, batchUpdate.CompressedTotalByQueue[queueB], uint64(0))

	queueAUncompressed := batchUpdate.UncompressedTotalByQueue[queueA]
	queueACompressed := batchUpdate.CompressedTotalByQueue[queueA]
	queueAEventBytes := uint64(len(batchUpdate.Events[0].Event))
	assert.Equal(t, queueAEventBytes, queueACompressed, "compressed total should match event bytes for queue-a")

	queueBUncompressed := batchUpdate.UncompressedTotalByQueue[queueB]
	queueBCompressed := batchUpdate.CompressedTotalByQueue[queueB]
	queueBEventBytes := uint64(len(batchUpdate.Events[1].Event))
	assert.Equal(t, queueBEventBytes, queueBCompressed, "compressed total should match event bytes for queue-b")

	assert.Greater(t, queueAUncompressed, uint64(0), "uncompressed size should be > 0 for queue-a")
	assert.Greater(t, queueBUncompressed, uint64(0), "uncompressed size should be > 0 for queue-b")
}

func TestConvertMetrics_MixedEventTypesLabelCorrectness(t *testing.T) {
	// Test that mixed event types in same batch correctly record metrics with separate labels
	t.Cleanup(func() {
		prometheus.Unregister(eventingestermetrics.UncompressedEventSizeMetric())
		prometheus.Unregister(eventingestermetrics.EstimatedCompressedEventSizeMetric())
	})

	// Create message with multiple event types in same sequence
	mixedMsg := NewMsg(jobRunSucceeded, cancelled)
	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), mixedMsg)

	assert.Equal(t, 1, len(batchUpdate.Events), "should have 1 event sequence after conversion")

	// Verify that both event types recorded metrics with correct (event_type, queue) labels
	jobRunSucceededUncompressed := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queue,
		),
	)
	assert.Greater(t, jobRunSucceededUncompressed, float64(0), "JobRunSucceeded should have recorded uncompressed size")

	cancelledUncompressed := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queue,
		),
	)
	assert.Greater(t, cancelledUncompressed, float64(0), "CancelledJob should have recorded uncompressed size")

	// Verify estimated compressed sizes are recorded separately by event type
	jobRunSucceededEstimated := testutil.ToFloat64(
		eventingestermetrics.EstimatedCompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queue,
		),
	)
	assert.Greater(t, jobRunSucceededEstimated, float64(0), "JobRunSucceeded should have recorded estimated compressed size")

	cancelledEstimated := testutil.ToFloat64(
		eventingestermetrics.EstimatedCompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queue,
		),
	)
	assert.Greater(t, cancelledEstimated, float64(0), "CancelledJob should have recorded estimated compressed size")

	// Verify label combination isolation: metrics are separate for each event type
	assert.NotEqual(t, jobRunSucceededUncompressed, cancelledUncompressed,
		"different event types should have different uncompressed sizes")
}

func TestConvertMetrics_MultiQueueBatchLabelIsolation(t *testing.T) {
	// Test that multiple queues in single logical batch correctly isolate metrics by queue label
	t.Cleanup(func() {
		prometheus.Unregister(eventingestermetrics.UncompressedEventSizeMetric())
		prometheus.Unregister(eventingestermetrics.EstimatedCompressedEventSizeMetric())
	})

	queueX := "queue-x"
	queueY := "queue-y"

	msgX := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      queueX,
				JobSetName: jobset,
				Events:     []*armadaevents.EventSequence_Event{jobRunSucceeded},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(1)},
	}

	msgY := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: []*armadaevents.EventSequence{
			{
				Queue:      queueY,
				JobSetName: jobset,
				Events:     []*armadaevents.EventSequence_Event{cancelled},
			},
		},
		MessageIds: []pulsar.MessageID{pulsarutils.NewMessageId(2)},
	}

	// Combine into single batch with multiple queues
	combined := &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events:     append(msgX.Events, msgY.Events...),
		MessageIds: append(msgX.MessageIds, msgY.MessageIds...),
	}

	converter := simpleEventConverter()
	batchUpdate := converter.Convert(armadacontext.Background(), combined)

	assert.Equal(t, 2, len(batchUpdate.Events), "should have 2 event sequences after conversion")

	// Verify event metrics are correctly labeled by queue
	jobRunSucceededQueueX := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queueX,
		),
	)
	assert.Greater(t, jobRunSucceededQueueX, float64(0), "JobRunSucceeded in queue-x should be recorded")

	cancelledQueueY := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queueY,
		),
	)
	assert.Greater(t, cancelledQueueY, float64(0), "CancelledJob in queue-y should be recorded")

	// Verify no cross-contamination: JobRunSucceeded should not appear under queue-y label
	jobRunSucceededQueueY := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_JobRunSucceeded",
			queueY,
		),
	)
	assert.Equal(t, float64(0), jobRunSucceededQueueY, "JobRunSucceeded should not exist under queue-y")

	// Verify no cross-contamination: CancelledJob should not appear under queue-x label
	cancelledQueueX := testutil.ToFloat64(
		eventingestermetrics.UncompressedEventSizeMetric().WithLabelValues(
			"*armadaevents.EventSequence_Event_CancelledJob",
			queueX,
		),
	)
	assert.Equal(t, float64(0), cancelledQueueX, "CancelledJob should not exist under queue-x")
}
