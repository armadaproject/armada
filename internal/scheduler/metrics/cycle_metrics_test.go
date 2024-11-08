package metrics

import (
	"fmt"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

const epsilon = 1e-6

func TestReportStateTransitions(t *testing.T) {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(
		cpu(100),
		configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"cpu"}},
	)
	require.NoError(t, err)
	result := scheduling.SchedulerResult{
		SchedulingContexts: []*context.SchedulingContext{
			{
				Pool:                 "pool1",
				FairnessCostProvider: fairnessCostProvider,
				QueueSchedulingContexts: map[string]*context.QueueSchedulingContext{
					"queue1": {
						Allocated:         cpu(10),
						Demand:            cpu(20),
						CappedDemand:      cpu(15),
						AdjustedFairShare: 0.15,
						SuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
							"job1": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
							"job2": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
						},
						UnsuccessfulJobSchedulingContexts: map[string]*context.JobSchedulingContext{
							"job2": {
								Job: testfixtures.Test1Cpu4GiJob("queue1", testfixtures.PriorityClass0),
							},
						},
					},
				},
			},
		},
	}

	m := newCycleMetrics()
	m.ReportSchedulerResult(result)

	poolQueue := []string{"pool1", "queue1"}

	consideredJobs := testutil.ToFloat64(m.consideredJobs.WithLabelValues(poolQueue...))
	assert.Equal(t, 3.0, consideredJobs, "consideredJobs")

	allocated := testutil.ToFloat64(m.actualShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.1, allocated, epsilon, "allocated")

	demand := testutil.ToFloat64(m.demand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.2, demand, epsilon, "demand")

	cappedDemand := testutil.ToFloat64(m.cappedDemand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, cappedDemand, epsilon, "cappedDemand")

	adjustedFairShare := testutil.ToFloat64(m.adjustedFairShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, adjustedFairShare, epsilon, "adjustedFairShare")

	fairnessError := testutil.ToFloat64(m.fairnessError.WithLabelValues("pool1"))
	assert.InDelta(t, 0.05, fairnessError, epsilon, "fairnessError")
}

func TestResetLeaderMetrics(t *testing.T) {
	m := newCycleMetrics()

	poolLabelValues := []string{"pool1"}
	poolQueueLabelValues := []string{"pool1", "queue1"}
	poolQueueResouceLabelValues := []string{"pool1", "queue1", "cpu"}
	queuePriorityClassLabelValues := []string{"pool1", "priorityClass1"}

	testResetCounter := func(vec *prometheus.CounterVec, labelValues []string) {
		vec.WithLabelValues(labelValues...).Inc()
		counterVal := testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 1.0, counterVal)
		m.resetLeaderMetrics()
		counterVal = testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 0.0, counterVal)
	}
	testResetGauge := func(vec *prometheus.GaugeVec, labelValues []string) {
		vec.WithLabelValues(labelValues...).Inc()
		counterVal := testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 1.0, counterVal)
		m.resetLeaderMetrics()
		counterVal = testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 0.0, counterVal)
	}

	testResetCounter(m.scheduledJobs, queuePriorityClassLabelValues)
	testResetCounter(m.premptedJobs, queuePriorityClassLabelValues)
	testResetGauge(m.consideredJobs, poolQueueLabelValues)
	testResetGauge(m.fairShare, poolQueueLabelValues)
	testResetGauge(m.adjustedFairShare, poolQueueLabelValues)
	testResetGauge(m.actualShare, poolQueueLabelValues)
	testResetGauge(m.fairnessError, []string{"pool1"})
	testResetGauge(m.demand, poolQueueLabelValues)
	testResetGauge(m.cappedDemand, poolQueueLabelValues)
	testResetGauge(m.gangsConsidered, poolQueueLabelValues)
	testResetGauge(m.gangsScheduled, poolQueueLabelValues)
	testResetGauge(m.firstGangQueuePosition, poolQueueLabelValues)
	testResetGauge(m.lastGangQueuePosition, poolQueueLabelValues)
	testResetGauge(m.perQueueCycleTime, poolQueueLabelValues)
	testResetGauge(m.loopNumber, poolLabelValues)
	testResetGauge(m.evictedJobs, poolQueueLabelValues)
	testResetGauge(m.evictedResources, poolQueueResouceLabelValues)
}

func TestDisableLeaderMetrics(t *testing.T) {
	m := newCycleMetrics()

	poolQueueLabelValues := []string{"pool1", "queue1"}
	queuePriorityClassLabelValues := []string{"pool1", "priorityClass1"}

	collect := func(m *cycleMetrics) []prometheus.Metric {
		m.scheduledJobs.WithLabelValues(queuePriorityClassLabelValues...).Inc()
		m.premptedJobs.WithLabelValues(queuePriorityClassLabelValues...).Inc()
		m.consideredJobs.WithLabelValues(poolQueueLabelValues...).Inc()
		m.fairShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.adjustedFairShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.actualShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.fairnessError.WithLabelValues("pool1").Inc()
		m.demand.WithLabelValues(poolQueueLabelValues...).Inc()
		m.cappedDemand.WithLabelValues(poolQueueLabelValues...).Inc()
		m.scheduleCycleTime.Observe(float64(1000))
		m.reconciliationCycleTime.Observe(float64(1000))
		m.gangsConsidered.WithLabelValues("pool1", "queue1").Inc()
		m.gangsScheduled.WithLabelValues("pool1", "queue1").Inc()
		m.firstGangQueuePosition.WithLabelValues("pool1", "queue1").Inc()
		m.lastGangQueuePosition.WithLabelValues("pool1", "queue1").Inc()
		m.perQueueCycleTime.WithLabelValues("pool1", "queue1").Inc()
		m.loopNumber.WithLabelValues("pool1").Inc()
		m.evictedJobs.WithLabelValues("pool1", "queue1").Inc()
		m.evictedResources.WithLabelValues("pool1", "queue1", "cpu").Inc()

		ch := make(chan prometheus.Metric, 1000)
		m.collect(ch)
		collected := make([]prometheus.Metric, 0, len(ch))
		for len(ch) > 0 {
			collected = append(collected, <-ch)
		}
		return collected
	}

	// Enabled
	assert.NotZero(t, len(collect(m)))

	// Disabled
	m.disableLeaderMetrics()
	assert.Equal(t, 1, len(collect(m)))

	// Enabled
	m.enableLeaderMetrics()
	assert.NotZero(t, len(collect(m)))
}

func cpu(n int) internaltypes.ResourceList {
	return testfixtures.TestResourceListFactory.FromJobResourceListIgnoreUnknown(
		map[string]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%d", n))},
	)
}
