package metrics

import (
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
	ArmadaLookoutIngesterV2MetricsPrefix = "armada_lookout_ingester_v2_"
	ArmadaEventIngesterMetricsPrefix     = "armada_event_ingester_"
)

const (
	JobSetEventsLabel       = "jobSet"
	ControlPlaneEventsLabel = "controlPlane"
)

type Metrics struct {
	dbErrorsCounter         *prometheus.CounterVec
	pulsarConnectionError   prometheus.Counter
	pulsarMessageError      *prometheus.CounterVec
	pulsarMessagesProcessed prometheus.Counter
	eventsProcessed         *prometheus.CounterVec
}

func NewMetrics(prefix string) *Metrics {
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
	eventsProcessedOpts := prometheus.CounterOpts{
		Name: prefix + "events_processed",
		Help: "Number of events processed",
	}

	return &Metrics{
		dbErrorsCounter:         promauto.NewCounterVec(dbErrorsCounterOpts, []string{"operation"}),
		pulsarMessageError:      promauto.NewCounterVec(pulsarMessageErrorOpts, []string{"error"}),
		pulsarConnectionError:   promauto.NewCounter(pulsarConnectionErrorOpts),
		pulsarMessagesProcessed: promauto.NewCounter(pulsarMessagesProcessedOpts),
		eventsProcessed:         promauto.NewCounterVec(eventsProcessedOpts, []string{"queue", "eventType", "msgType"}),
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

func (m *Metrics) RecordEventSequenceProcessed(queue string, msgType string) {
	m.eventsProcessed.With(map[string]string{"queue": queue, "eventType": JobSetEventsLabel, "msgType": msgType}).Inc()
}

func (m *Metrics) RecordControlPlaneEventProcessed(msgType string) {
	m.eventsProcessed.With(map[string]string{"queue": "N/A", "eventType": ControlPlaneEventsLabel, "msgType": msgType}).Inc()
}
