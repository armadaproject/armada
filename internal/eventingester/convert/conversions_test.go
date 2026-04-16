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
	converter := NewEventConverter(compressor, 1024, testMetrics, true)

	succeededType := jobRunSucceeded.GetEventName()
	cancelledType := cancelled.GetEventName()
	failedType := jobRunFailed.GetEventName()

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 1, len(batchUpdate.Events))

	assert.Greater(t, testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)), float64(0))
	assert.Greater(t, testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)), float64(0))
	assert.Greater(t, testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, failedType)), float64(0))
	assert.Greater(t, testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)), float64(0))
	assert.Greater(t, testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)), float64(0))
	assert.Greater(t, testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, failedType)), float64(0))
}

func TestConvert_DoesNotRecordSizeMetricsWhenCompressionFails(t *testing.T) {
	msg := NewMsg(jobRunSucceeded, cancelled)
	failingCompressor := &failingCompressor{}
	testRegistry := prometheus.NewRegistry()
	testMetrics := metrics.NewMetricsWithRegistry("test_failure_path_", testRegistry)
	converter := NewEventConverter(failingCompressor, 1024, testMetrics, true)

	succeededType := jobRunSucceeded.GetEventName()
	cancelledType := cancelled.GetEventName()

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 0, len(batchUpdate.Events))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)))
}

func TestConvert_DoesNotRecordSizeMetrics_WhenEventSizeMetricsDisabled(t *testing.T) {
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
	converter := NewEventConverter(compressor, 1024, testMetrics, false)

	succeededType := jobRunSucceeded.GetEventName()
	cancelledType := cancelled.GetEventName()
	failedType := jobRunFailed.GetEventName()

	batchUpdate := converter.Convert(armadacontext.Background(), msg)
	require.Equal(t, 1, len(batchUpdate.Events))

	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, succeededType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, cancelledType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetUncompressedEventBytesTotal().WithLabelValues(queue, failedType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, succeededType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, cancelledType)))
	assert.Equal(t, float64(0), testutil.ToFloat64(testMetrics.GetEstimatedCompressedEventBytesTotal().WithLabelValues(queue, failedType)))
}

type failingCompressor struct{}

func (f *failingCompressor) Compress([]byte) ([]byte, error) {
	return nil, errors.New("intentional compression failure")
}
