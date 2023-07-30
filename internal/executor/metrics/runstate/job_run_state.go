package runstate

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/armadaproject/armada/internal/executor/job"
	"github.com/armadaproject/armada/internal/executor/metrics"
)

const (
	queueLabel = "queue"
	phaseLabel = "phase"
)

var runPhaseCountDesc = prometheus.NewDesc(
	metrics.ArmadaExecutorMetricsPrefix+"run_phase",
	"Runs in different phases by queue",
	[]string{queueLabel, phaseLabel}, nil,
)

type JobRunStateStoreMetricsCollector struct {
	jobRunStateStore job.RunStateStore
}

func NewJobRunStateStoreMetricsCollector(jobRunStateStore job.RunStateStore) *JobRunStateStoreMetricsCollector {
	collector := &JobRunStateStoreMetricsCollector{
		jobRunStateStore: jobRunStateStore,
	}
	return collector
}

func (j *JobRunStateStoreMetricsCollector) Describe(desc chan<- *prometheus.Desc) {
	desc <- runPhaseCountDesc
}

type runStateKey struct {
	Queue string
	Phase job.RunPhase
}

func (j *JobRunStateStoreMetricsCollector) Collect(metrics chan<- prometheus.Metric) {
	runs := j.jobRunStateStore.GetAll()

	phaseCountByQueue := map[runStateKey]int{}

	for _, run := range runs {
		key := runStateKey{
			Queue: run.Meta.Queue,
			Phase: run.Phase,
		}
		phaseCountByQueue[key]++
	}

	for metricKey, value := range phaseCountByQueue {
		metrics <- prometheus.MustNewConstMetric(runPhaseCountDesc, prometheus.GaugeValue,
			float64(value), metricKey.Queue, metricKey.Phase.String())
	}
}
