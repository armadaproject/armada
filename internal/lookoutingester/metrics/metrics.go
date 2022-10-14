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
	ArmadaLookoutIngesterMetricsPrefix = "armada_lookout_ingester_"
)

var m = newMetrics()

type Metrics struct {
	pulsarConnectionErrorsCounter  prometheus.Counter
	dbErrorsCounter                *prometheus.CounterVec
	pulsarMessageError             *prometheus.CounterVec
	messageProcessingErrorsCounter prometheus.Counter
}

func Get() *Metrics {
	return m
}

func newMetrics() *Metrics {
	pulsarConnectionErrorsCounterOpts := prometheus.CounterOpts{
		Name: ArmadaLookoutIngesterMetricsPrefix + "pulsar_connection_errors",
		Help: "Number of Pulsar connection errors",
	}
	dbErrorsCounterOpts := prometheus.CounterOpts{
		Name: ArmadaLookoutIngesterMetricsPrefix + "db_errors",
		Help: "Number of database errors",
	}
	pulsarMessageErrorOpts := prometheus.CounterOpts{
		Name: ArmadaLookoutIngesterMetricsPrefix + "pulsar_message_error",
		Help: "Number of message deserialization errors",
	}
	return &Metrics{
		pulsarConnectionErrorsCounter: promauto.NewCounter(pulsarConnectionErrorsCounterOpts),
		dbErrorsCounter:               promauto.NewCounterVec(dbErrorsCounterOpts, []string{"operation"}),
		pulsarMessageError:            promauto.NewCounterVec(pulsarMessageErrorOpts, []string{"error"}),
	}
}

func (m *Metrics) RecordPulsarConnectionError() {
	m.pulsarConnectionErrorsCounter.Inc()
}

func (m *Metrics) RecordDBError(operation DBOperation) {
	m.dbErrorsCounter.With(map[string]string{"operation": string(operation)}).Inc()
}

func (m *Metrics) RecordPulsarMessageError(error PulsarMessageError) {
	m.pulsarMessageError.With(map[string]string{"error": string(error)}).Inc()
}

func (m *Metrics) RecordMessageProcessingError() {
	m.messageProcessingErrorsCounter.Inc()
}
