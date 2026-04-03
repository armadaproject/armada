package metrics

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type (
	DBOperation        string
	PulsarMessageError string
)

const (
	DBOperationRead                   DBOperation        = "read"
	DBOperationInsert                 DBOperation        = "insert"
	DBOperationUpdate                 DBOperation        = "update"
	DBOperationCreateTempTable        DBOperation        = "create_temp_table"
	PulsarMessageErrorDeserialization PulsarMessageError = "deserialization"
	PulsarMessageErrorProcessing      PulsarMessageError = "processing"
)

const (
	ArmadaLookoutIngesterMetricsPrefix = "armada_lookout_ingester_v2_"
	ArmadaEventIngesterMetricsPrefix   = "armada_event_ingester_"
)

const (
	JobSetEventsLabel       = "jobSet"
	ControlPlaneEventsLabel = "controlPlane"
)

type Metrics struct {
	dbErrorsCounter                    *prometheus.CounterVec
	pulsarConnectionError              prometheus.Counter
	pulsarMessageError                 *prometheus.CounterVec
	pulsarMessagesProcessed            prometheus.Counter
	pulsarMessagePublishTime           *prometheus.GaugeVec
	pulsarMessageProcessingDelay       *prometheus.GaugeVec
	eventsProcessed                    *prometheus.CounterVec
	uncompressedEventBytesTotal        *prometheus.CounterVec
	estimatedCompressedEventBytesTotal *prometheus.CounterVec
	batchEvents                        *prometheus.HistogramVec
	batchesTotal                       *prometheus.CounterVec
}

func NewMetrics(prefix string) *Metrics {
	return NewMetricsWithRegistry(prefix, prometheus.DefaultRegisterer)
}

func NewMetricsWithRegistry(prefix string, registerer prometheus.Registerer) *Metrics {
	dbErrorsCounterOpts := prometheus.CounterOpts{
		Name: prefix + "db_errors",
		Help: "Number of database errors grouped by database operation",
	}
	pulsarMessageErrorOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_message_errors",
		Help: "Number of Pulsar message errors grouped by error type",
	}
	pulsarConnectionErrorOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_connection_errors",
		Help: "Number of Pulsar connection errors",
	}
	pulsarMessagesProcessedOpts := prometheus.CounterOpts{
		Name: prefix + "pulsar_messages_processed",
		Help: "Number of pulsar messages processed",
	}
	pulsarMessagePublishTime := prometheus.GaugeOpts{
		Name: prefix + "pulsar_message_publish_time",
		Help: "Publish time of pulsar message being processed",
	}
	pulsarMessageProcessingDelayOpts := prometheus.GaugeOpts{
		Name: prefix + "pulsar_message_processing_delay",
		Help: "Delay in ms of pulsar messages",
	}
	eventsProcessedOpts := prometheus.CounterOpts{
		Name: prefix + "events_processed",
		Help: "Number of events processed",
	}
	uncompressedEventBytesTotalOpts := prometheus.CounterOpts{
		Name: prefix + "uncompressed_event_bytes_total",
		Help: "Total uncompressed event bytes processed",
	}
	estimatedCompressedEventBytesTotalOpts := prometheus.CounterOpts{
		Name: prefix + "estimated_compressed_event_bytes_total",
		Help: "Total estimated compressed event bytes processed",
	}
	batchEventsOpts := prometheus.HistogramOpts{
		Name:    prefix + "batch_events",
		Help:    "Number of events in a batch",
		Buckets: []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000},
	}
	batchesTotalOpts := prometheus.CounterOpts{
		Name: prefix + "batches_total",
		Help: "Total number of batches processed",
	}

	factory := promauto.With(registerer)
	return &Metrics{
		dbErrorsCounter:                    factory.NewCounterVec(dbErrorsCounterOpts, []string{"operation"}),
		pulsarMessageError:                 factory.NewCounterVec(pulsarMessageErrorOpts, []string{"error"}),
		pulsarConnectionError:              factory.NewCounter(pulsarConnectionErrorOpts),
		pulsarMessageProcessingDelay:       factory.NewGaugeVec(pulsarMessageProcessingDelayOpts, []string{"subscription", "partition"}),
		pulsarMessagePublishTime:           factory.NewGaugeVec(pulsarMessagePublishTime, []string{"subscription", "partition"}),
		pulsarMessagesProcessed:            factory.NewCounter(pulsarMessagesProcessedOpts),
		eventsProcessed:                    factory.NewCounterVec(eventsProcessedOpts, []string{"queue", "eventType", "msgType"}),
		uncompressedEventBytesTotal:        factory.NewCounterVec(uncompressedEventBytesTotalOpts, []string{"queue", "event_type"}),
		estimatedCompressedEventBytesTotal: factory.NewCounterVec(estimatedCompressedEventBytesTotalOpts, []string{"queue", "event_type"}),
		batchEvents:                        factory.NewHistogramVec(batchEventsOpts, []string{"queue"}),
		batchesTotal:                       factory.NewCounterVec(batchesTotalOpts, []string{"queue"}),
	}
}

func (m *Metrics) RecordDBError(operation DBOperation) {
	m.dbErrorsCounter.With(map[string]string{"operation": string(operation)}).Inc()
}

func (m *Metrics) RecordPulsarMessageError(error PulsarMessageError) {
	m.pulsarMessageError.With(map[string]string{"error": string(error)}).Inc()
}

func (m *Metrics) RecordPulsarConnectionError() {
	m.pulsarConnectionError.Inc()
}

func (m *Metrics) RecordPulsarMessageProcessed() {
	m.pulsarMessagesProcessed.Inc()
}

func (m *Metrics) RecordPulsarMessagePublishTime(subscriptionName string, partition int, publishTime time.Time) {
	partitionStr := strconv.Itoa(partition)
	m.pulsarMessagePublishTime.WithLabelValues(subscriptionName, partitionStr).Set(float64(publishTime.UTC().Unix()))
}

func (m *Metrics) RecordPulsarProcessingDelay(subscriptionName string, partition int, delayInMs float64) {
	partitionStr := strconv.Itoa(partition)
	m.pulsarMessageProcessingDelay.WithLabelValues(subscriptionName, partitionStr).Set(delayInMs)
}

func (m *Metrics) RecordEventSequenceProcessed(queue string, msgType string) {
	m.eventsProcessed.With(map[string]string{"queue": queue, "eventType": JobSetEventsLabel, "msgType": msgType}).Inc()
}

func (m *Metrics) RecordControlPlaneEventProcessed(msgType string) {
	m.eventsProcessed.With(map[string]string{"queue": "N/A", "eventType": ControlPlaneEventsLabel, "msgType": msgType}).Inc()
}

func (m *Metrics) RecordEventUncompressedBytes(queue, eventType string, n int) {
	m.uncompressedEventBytesTotal.With(map[string]string{"queue": queue, "event_type": eventType}).Add(float64(n))
}

func (m *Metrics) RecordEventEstimatedCompressedBytes(queue, eventType string, n int) {
	m.estimatedCompressedEventBytesTotal.With(map[string]string{"queue": queue, "event_type": eventType}).Add(float64(n))
}

func (m *Metrics) RecordBatchEvents(queue string, batchEvents int) {
	m.batchEvents.With(map[string]string{"queue": queue}).Observe(float64(batchEvents))
}

func (m *Metrics) RecordBatchCount(queue string) {
	m.batchesTotal.With(map[string]string{"queue": queue}).Inc()
}

func (m *Metrics) GetUncompressedEventBytesTotal() *prometheus.CounterVec {
	return m.uncompressedEventBytesTotal
}

func (m *Metrics) GetEstimatedCompressedEventBytesTotal() *prometheus.CounterVec {
	return m.estimatedCompressedEventBytesTotal
}

func (m *Metrics) GetBatchEvents() *prometheus.HistogramVec {
	return m.batchEvents
}

func (m *Metrics) GetBatchesTotal() *prometheus.CounterVec {
	return m.batchesTotal
}
