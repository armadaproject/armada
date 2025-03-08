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
						Allocated:                     cpu(10),
						Demand:                        cpu(20),
						ConstrainedDemand:             cpu(15),
						DemandCappedAdjustedFairShare: 0.15,
						UncappedAdjustedFairShare:     0.2,
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

	consideredJobs := testutil.ToFloat64(m.latestCycleMetrics.Load().consideredJobs.WithLabelValues(poolQueue...))
	assert.Equal(t, 3.0, consideredJobs, "consideredJobs")

	allocated := testutil.ToFloat64(m.latestCycleMetrics.Load().actualShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.1, allocated, epsilon, "allocated")

	demand := testutil.ToFloat64(m.latestCycleMetrics.Load().demand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.2, demand, epsilon, "demand")

	constrainedDemand := testutil.ToFloat64(m.latestCycleMetrics.Load().constrainedDemand.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, constrainedDemand, epsilon, "constrainedDemand")

	adjustedFairShare := testutil.ToFloat64(m.latestCycleMetrics.Load().adjustedFairShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.15, adjustedFairShare, epsilon, "adjustedFairShare")

	uncappedAdjustedFairShare := testutil.ToFloat64(m.latestCycleMetrics.Load().uncappedAdjustedFairShare.WithLabelValues(poolQueue...))
	assert.InDelta(t, 0.2, uncappedAdjustedFairShare, epsilon, "uncappedAdjustedFairShare")

	fairnessError := testutil.ToFloat64(m.latestCycleMetrics.Load().fairnessError.WithLabelValues("pool1"))
	assert.InDelta(t, 0.05, fairnessError, epsilon, "fairnessError")
}

func TestResetLeaderMetrics_Counters(t *testing.T) {
	m := newCycleMetrics()
	poolAndQueueAndPriorityClassTypeLabels := []string{"pool1", "queue1", "priorityClass1", "type1"}

	testResetCounter := func(vec *prometheus.CounterVec, labelValues []string) {
		vec.WithLabelValues(labelValues...).Inc()
		counterVal := testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 1.0, counterVal)
		m.resetLeaderMetrics()
		counterVal = testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 0.0, counterVal)
	}

	testResetCounter(m.scheduledJobs, poolAndQueueAndPriorityClassTypeLabels)
	testResetCounter(m.premptedJobs, poolAndQueueAndPriorityClassTypeLabels)
}

func TestResetLeaderMetrics_ResetsLatestCycleMetrics(t *testing.T) {
	m := newCycleMetrics()
	poolLabelValues := []string{"pool1"}
	poolQueueLabelValues := []string{"pool1", "queue1"}
	poolQueueResourceLabelValues := []string{"pool1", "queue1", "cpu"}
	nodeResourceLabelValues := []string{"pool1", "node1", "cluster1", "type1", "cpu", "true"}

	testResetGauge := func(getVec func(metrics *cycleMetrics) *prometheus.GaugeVec, labelValues []string) {
		vec := getVec(m)
		vec.WithLabelValues(labelValues...).Inc()
		counterVal := testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 1.0, counterVal)
		m.resetLeaderMetrics()
		vec = getVec(m)
		counterVal = testutil.ToFloat64(vec.WithLabelValues(labelValues...))
		assert.Equal(t, 0.0, counterVal)
	}

	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().consideredJobs }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().fairShare }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().adjustedFairShare }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().actualShare }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().fairnessError }, []string{"pool1"})
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().demand }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().constrainedDemand }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().gangsConsidered }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().gangsScheduled }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec {
		return m.latestCycleMetrics.Load().firstGangQueuePosition
	}, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec {
		return m.latestCycleMetrics.Load().lastGangQueuePosition
	}, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().perQueueCycleTime }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().loopNumber }, poolLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().evictedJobs }, poolQueueLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec { return m.latestCycleMetrics.Load().evictedResources }, poolQueueResourceLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec {
		return m.latestCycleMetrics.Load().nodeAllocatableResource
	}, nodeResourceLabelValues)
	testResetGauge(func(metrics *cycleMetrics) *prometheus.GaugeVec {
		return m.latestCycleMetrics.Load().nodeAllocatedResource
	}, nodeResourceLabelValues)
}

func TestDisableLeaderMetrics(t *testing.T) {
	m := newCycleMetrics()

	poolQueueLabelValues := []string{"pool1", "queue1"}
	poolAndQueueAndPriorityClassTypeLabels := []string{"pool1", "queue1", "priorityClass1", "type1"}

	collect := func(m *cycleMetrics) []prometheus.Metric {
		m.scheduledJobs.WithLabelValues(poolAndQueueAndPriorityClassTypeLabels...).Inc()
		m.premptedJobs.WithLabelValues(poolAndQueueAndPriorityClassTypeLabels...).Inc()
		m.latestCycleMetrics.Load().consideredJobs.WithLabelValues(poolQueueLabelValues...).Inc()
		m.latestCycleMetrics.Load().fairShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.latestCycleMetrics.Load().adjustedFairShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.latestCycleMetrics.Load().actualShare.WithLabelValues(poolQueueLabelValues...).Inc()
		m.latestCycleMetrics.Load().fairnessError.WithLabelValues("pool1").Inc()
		m.latestCycleMetrics.Load().demand.WithLabelValues(poolQueueLabelValues...).Inc()
		m.latestCycleMetrics.Load().constrainedDemand.WithLabelValues(poolQueueLabelValues...).Inc()
		m.scheduleCycleTime.Observe(float64(1000))
		m.reconciliationCycleTime.Observe(float64(1000))
		m.latestCycleMetrics.Load().gangsConsidered.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().gangsScheduled.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().firstGangQueuePosition.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().lastGangQueuePosition.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().perQueueCycleTime.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().loopNumber.WithLabelValues("pool1").Inc()
		m.latestCycleMetrics.Load().evictedJobs.WithLabelValues("pool1", "queue1").Inc()
		m.latestCycleMetrics.Load().evictedResources.WithLabelValues("pool1", "queue1", "cpu").Inc()
		m.latestCycleMetrics.Load().nodeAllocatableResource.WithLabelValues("pool1", "node1", "cluster1", "type1", "cpu", "true").Inc()
		m.latestCycleMetrics.Load().nodeAllocatedResource.WithLabelValues("pool1", "node1", "cluster1", "type1", "cpu", "true").Inc()

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
