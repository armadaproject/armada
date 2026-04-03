package convert

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	jobset = "testJobset"
	queue  = "testQueue"
	JobId  = "01f3j0g1md4qx7z5qb148qnh4r"
	RunId  = "123e4567-e89b-12d3-a456-426614174000"
)

var (
	baseTime, _      = time.Parse("2006-01-02T15:04:05.000Z", "2022-03-01T15:04:05.000Z")
	baseTimeProto    = protoutil.ToTimestamp(baseTime)
	metricsTestCount = 0
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
	metricsTestCount++
	compressor, _ := compress.NewZlibCompressor(0)
	testMetrics := metrics.NewMetrics(fmt.Sprintf("test_simple_%d_", metricsTestCount))
	return &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: 1024,
		metrics:             testMetrics,
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

func TestConvert_RecordsEventSizeMetricsPerTypeAndQueue(t *testing.T) {
	jobRunFailed := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId: RunId,
				JobId: JobId,
			},
		},
	}

	msg := NewMsg(jobRunSucceeded, cancelled, jobRunFailed)
	compressor, _ := compress.NewZlibCompressor(0)
	testRegistry := prometheus.NewRegistry()
	testMetrics := metrics.NewMetricsWithRegistry("test_happy_path_", testRegistry)
	converter := &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: 1024,
		metrics:             testMetrics,
	}

	succeededType := jobRunSucceeded.GetEventName()
	cancelledType := cancelled.GetEventName()
	failedType := jobRunFailed.GetEventName()

	uncompressedBefore := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
		failedType:    testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, failedType)),
	}
	compressedBefore := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
		failedType:    testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, failedType)),
	}

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 1, len(batchUpdate.Events))

	uncompressedAfter := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
		failedType:    testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, failedType)),
	}
	compressedAfter := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
		failedType:    testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, failedType)),
	}

	for _, eventType := range []string{succeededType, cancelledType, failedType} {
		deltaUncompressed := uncompressedAfter[eventType] - uncompressedBefore[eventType]
		deltaCompressed := compressedAfter[eventType] - compressedBefore[eventType]

		assert.Greater(t, deltaUncompressed, 0.0, "uncompressed bytes should increase for %s", eventType)
		assert.Greater(t, deltaCompressed, 0.0, "estimated compressed bytes should increase for %s", eventType)
		assert.LessOrEqual(t, deltaCompressed, deltaUncompressed, "compressed should be <= uncompressed for %s", eventType)
	}
}

func TestConvert_DoesNotRecordSizeMetricsWhenCompressionFails(t *testing.T) {
	msg := NewMsg(jobRunSucceeded, cancelled)
	failingCompressor := &failingCompressor{}
	testRegistry := prometheus.NewRegistry()
	testMetrics := metrics.NewMetricsWithRegistry("test_failure_path_", testRegistry)
	converter := &EventConverter{
		Compressor:          failingCompressor,
		MaxMessageBatchSize: 1024,
		metrics:             testMetrics,
	}

	succeededType := jobRunSucceeded.GetEventName()
	cancelledType := cancelled.GetEventName()

	uncompressedBefore := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
	}
	compressedBefore := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
	}

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 0, len(batchUpdate.Events))

	uncompressedAfter := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
	}
	compressedAfter := map[string]float64{
		succeededType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)),
		cancelledType: testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)),
	}

	for _, eventType := range []string{succeededType, cancelledType} {
		assert.Equal(t, uncompressedBefore[eventType], uncompressedAfter[eventType], "uncompressed bytes should not change on compression failure for %s", eventType)
		assert.Equal(t, compressedBefore[eventType], compressedAfter[eventType], "compressed bytes should not change on compression failure for %s", eventType)
	}
}

type failingCompressor struct{}

func (f *failingCompressor) Compress([]byte) ([]byte, error) {
	return nil, errors.New("intentional compression failure")
}

func TestConvert_RecordsBatchHistogramAndCountPerSuccessfulSequence(t *testing.T) {
	jobRunFailed := &armadaevents.EventSequence_Event{
		Created: baseTimeProto,
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId: RunId,
				JobId: JobId,
			},
		},
	}

	msg := NewMsg(jobRunSucceeded, cancelled, jobRunFailed)
	compressor, _ := compress.NewZlibCompressor(0)
	testRegistry := prometheus.NewRegistry()
	testMetrics := metrics.NewMetricsWithRegistry("test_batch_happy_path_", testRegistry)
	converter := &EventConverter{
		Compressor:          compressor,
		MaxMessageBatchSize: 1024,
		metrics:             testMetrics,
	}

	batchCountBefore := testutil.ToFloat64(testMetrics.GetBatchesTotal().WithLabelValues(queue))

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 1, len(batchUpdate.Events))

	batchCountAfter := testutil.ToFloat64(testMetrics.GetBatchesTotal().WithLabelValues(queue))

	assert.Equal(t, batchCountBefore+1, batchCountAfter, "batch count should increment by 1")

	batchEventsCollector := testMetrics.GetBatchEvents()
	ch := make(chan prometheus.Metric, 100)
	go func() {
		batchEventsCollector.Collect(ch)
		close(ch)
	}()

	var histogramFound bool
	for m := range ch {
		pb := &dto.Metric{}
		require.NoError(t, m.Write(pb))

		if len(pb.Label) > 0 && pb.Label[0].GetValue() == queue && pb.Histogram != nil {
			histogramFound = true
			assert.Equal(t, uint64(1), pb.Histogram.GetSampleCount(), "histogram sample count should be 1")
			assert.Equal(t, float64(3), pb.Histogram.GetSampleSum(), "histogram sample sum should be 3 (3 events)")
			break
		}
	}
	assert.True(t, histogramFound, "histogram metric for queue should be found")
}

func TestConvert_DoesNotRecordBatchMetricsOnFailedSequence(t *testing.T) {
	msg := NewMsg(jobRunSucceeded, cancelled)
	failingCompressor := &failingCompressor{}
	testRegistry := prometheus.NewRegistry()
	testMetrics := metrics.NewMetricsWithRegistry("test_batch_failure_path_", testRegistry)
	converter := &EventConverter{
		Compressor:          failingCompressor,
		MaxMessageBatchSize: 1024,
		metrics:             testMetrics,
	}

	batchCountBefore := testutil.ToFloat64(testMetrics.GetBatchesTotal().WithLabelValues(queue))

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 0, len(batchUpdate.Events))

	batchCountAfter := testutil.ToFloat64(testMetrics.GetBatchesTotal().WithLabelValues(queue))

	assert.Equal(t, batchCountBefore, batchCountAfter, "batch count should not change on compression failure")

	batchEventsCollector := testMetrics.GetBatchEvents()
	ch := make(chan prometheus.Metric, 100)
	go func() {
		batchEventsCollector.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		pb := &dto.Metric{}
		require.NoError(t, m.Write(pb))

		if len(pb.Label) > 0 && pb.Label[0].GetValue() == queue && pb.Histogram != nil {
			assert.Equal(t, uint64(0), pb.Histogram.GetSampleCount(), "histogram count should be 0 on compression failure")
			break
		}
	}
}
