package scheduler

import (
	"context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/google/uuid"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/clock"

	commonmetrics "github.com/armadaproject/armada/internal/common/metrics"
	"github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
)

// stores the metrics state associated with a queue
type queueState struct {
	numQueuedJobs      int
	queuedJobRecorder  *commonmetrics.JobMetricsRecorder
	runningJobRecorder *commonmetrics.JobMetricsRecorder
}

// a snapshot of metrics.  Implements QueueMetricProvider
type metricsState struct {
	queues      []*database.Queue
	queueStates map[string]*queueState
}

func (m metricsState) GetQueuedJobMetrics(queueName string) []*commonmetrics.QueueMetrics {
	state, ok := m.queueStates[queueName]
	if ok {
		return state.queuedJobRecorder.Metrics()
	}
	return nil
}

func (m metricsState) GetRunningJobMetrics(queueName string) []*commonmetrics.QueueMetrics {
	state, ok := m.queueStates[queueName]
	if ok {
		return state.runningJobRecorder.Metrics()
	}
	return nil
}

func (m metricsState) numQueuedJobs() map[string]int {
	queueCounts := make(map[string]int)
	for _, queue := range m.queues {
		state, ok := m.queueStates[queue.Name]
		count := 0
		if ok {
			count = state.numQueuedJobs
		}
		queueCounts[queue.Name] = count
	}
	return queueCounts
}

// MetricsCollector is a Prometheus Collector that handles scheduler metrics.
// The metrics themselves are calculated asynchronously every refreshPeriod
type MetricsCollector struct {
	jobDb              *jobdb.JobDb
	queueRepository    database.QueueRepository
	executorRepository database.ExecutorRepository
	poolAssigner       PoolAssigner
	refreshPeriod      time.Duration
	clock              clock.Clock
	state              atomic.Value
}

func NewMetricsCollector(
	jobDb *jobdb.JobDb,
	queueRepository database.QueueRepository,
	executorRepository database.ExecutorRepository,
	poolAssigner PoolAssigner,
	refreshPeriod time.Duration,
) *MetricsCollector {
	return &MetricsCollector{
		jobDb:              jobDb,
		queueRepository:    queueRepository,
		executorRepository: executorRepository,
		poolAssigner:       poolAssigner,
		refreshPeriod:      refreshPeriod,
		clock:              clock.RealClock{},
		state:              atomic.Value{},
	}
}

// Run enters s a loop which updates the metrics every refreshPeriod until the supplied context is cancelled
func (c *MetricsCollector) Run(ctx context.Context) error {
	ticker := c.clock.NewTicker(c.refreshPeriod)
	log.Infof("Will update metrics every %s", c.refreshPeriod)
	for {
		select {
		case <-ctx.Done():
			log.Infof("Context cancelled, returning..")
			return nil
		case <-ticker.C():
			err := c.refresh(ctx)
			if err != nil {
				log.WithError(err).Warnf("error refreshing metrics state")
			}
		}
	}
}

// Describe returns all descriptions of the collector.
func (c *MetricsCollector) Describe(out chan<- *prometheus.Desc) {
	commonmetrics.Describe(out)
}

// Collect returns the current state of all metrics of the collector.
func (c *MetricsCollector) Collect(metrics chan<- prometheus.Metric) {
	state, ok := c.state.Load().(metricsState)
	if ok {

	}
}

func (c *MetricsCollector) refresh(ctx context.Context) error {
	log.Debugf("Refreshing prometheus metrics")
	start := time.Now()

	queues, err := c.queueRepository.GetAllQueues()
	if err != nil {
		return err
	}

	executors, err := c.executorRepository.GetExecutors(ctx)
	if err != nil {
		return err
	}

	err = c.poolAssigner.Refresh(ctx)
	if err != nil {
		return err
	}

	ms := metricsState{
		queues:      queues,
		queueStates: map[string]*queueState{},
	}
	for _, queue := range queues {
		ms.queueStates[queue.Name] = &queueState{
			queuedJobRecorder:  commonmetrics.NewJobMetricsRecorder(),
			runningJobRecorder: commonmetrics.NewJobMetricsRecorder(),
		}
	}

	currentTime := c.clock.Now()
	txn := c.jobDb.ReadTxn()
	for _, job := range c.jobDb.GetAll(txn) {
		// Don't calculate metrics for dead jobs
		if job.InTerminalState() {
			continue
		}
		qs, ok := ms.queueStates[job.Queue()]
		if !ok {
			log.Warnf("Job %s is in queue %s, but this queue does not exist.  Skipping", job.Id(), job.Queue())
			continue
		}

		pool, err := c.poolAssigner.AssignPool(job)
		if err != nil {
			return err
		}

		priorityClass := job.JobSchedulingInfo().PriorityClassName
		resourceRequirements := job.JobSchedulingInfo().GetObjectRequirements()[0].GetPodRequirements().GetResourceRequirements().Requests
		jobResources := make(map[string]float64)
		for key, value := range resourceRequirements {
			jobResources[string(key)] = resource.QuantityAsFloat64(value)
		}

		var recorder *commonmetrics.JobMetricsRecorder
		var timeInState time.Duration
		if job.Queued() {
			recorder = qs.queuedJobRecorder
			timeInState = currentTime.Sub(time.Unix(0, job.Created()))
			qs.numQueuedJobs++
		} else if job.HasRuns() {
			run := job.LatestRun()
			timeInState = currentTime.Sub(time.Unix(0, run.Created()))
			recorder = qs.runningJobRecorder
		} else {
			log.Warnf("Job %s is marked as leased but has no runs", job.Id())
		}
		recorder.RecordJobRuntime(pool, priorityClass, timeInState)
		recorder.RecordResources(pool, priorityClass, jobResources)
	}

	queueMetrics := commonmetrics.CollectQueueMetrics(ms.numQueuedJobs(), ms)

	type phaseKey struct {
		cluster   string
		pool      string
		queueName string
		phase     string
		nodeType  string
	}
	countsByKey := map[phaseKey]int{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			for runId, jobRunState := range node.StateByJobRunId {
				job := c.jobDb.GetByRunId(txn, uuid.MustParse(runId))
				if job != nil {
					phase := schedulerobjects.JobRunState_name[int32(jobRunState)]
					key := phaseKey{
						cluster:   executor.Id,
						pool:      executor.Pool,
						queueName: job.Queue(),
						phase:     phase,
						nodeType:  "",
					}
					countsByKey[key]++
				}
			}
		}
	}

	c.state.Store(ms)
	log.Debugf("Refreshed prometheus metrics in %s", time.Since(start))
	return nil
}
