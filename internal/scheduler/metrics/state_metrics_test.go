package metrics

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const (
	testPool          = "testPool"
	testNode          = "testNode"
	testCluster       = "testCluster"
	testQueue         = testfixtures.TestQueue
	testPriorityClass = testfixtures.PriorityClass0
)

var (
	baseTime = time.Now()

	baseRun = jobdb.MinimalRun(uuid.NewString(), baseTime.UnixNano()).
		WithPool(testPool).WithNodeName(testNode).
		WithExecutor(testCluster)

	baseJob = testfixtures.Test16Cpu128GiJob(testQueue, testPriorityClass).
		WithSubmittedTime(baseTime.UnixNano())
)

func TestReportJobStateTransitions(t *testing.T) {
	baseTimePlusSeconds := func(numSeconds int) *time.Time {
		newTime := baseTime.Add(time.Duration(numSeconds) * time.Second)
		return &newTime
	}

	tests := map[string]struct {
		errorRegexes                           []*regexp.Regexp
		trackedResourceNames                   []v1.ResourceName
		jsts                                   []jobdb.JobStateTransitions
		jobRunErrorsByRunId                    map[string]*armadaevents.Error
		expectedJobStateCounterByQueue         map[[4]string]float64
		expectedJobStateCounterByNode          map[[5]string]float64
		expectedJobStateSecondsByQueue         map[[4]string]float64
		expectedJobStateSecondsByNode          map[[5]string]float64
		expectedJobStateResourceSecondsByQueue map[[5]string]float64
		expectedJobStateResourceSecondsByNode  map[[6]string]float64
	}{
		"Pending": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62))),
					Pending: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "pending", "leased"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "pending", "leased"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "pending", "leased"}: 2,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "pending", "leased"}: 2,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "pending", "leased", "cpu"}: 2 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "pending", "leased", "cpu"}: 2 * 16,
			},
		},
		"Running": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72))),
					Running: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "running", "pending"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "running", "pending"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "running", "pending"}: 10,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "running", "pending"}: 10,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "running", "pending", "cpu"}: 10 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "running", "pending", "cpu"}: 10 * 16,
			},
		},
		"Succeeded": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Succeeded: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "succeeded", "running"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "succeeded", "running"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "succeeded", "running"}: 8,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "succeeded", "running"}: 8,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "succeeded", "running", "cpu"}: 8 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "succeeded", "running", "cpu"}: 8 * 16,
			},
		},
		"Cancelled": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Cancelled: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "cancelled", "running"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "cancelled", "running"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "cancelled", "running"}: 8,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "cancelled", "running"}: 8,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "cancelled", "running", "cpu"}: 8 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "cancelled", "running", "cpu"}: 8 * 16,
			},
		},
		"Failed": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithTerminatedTime(baseTimePlusSeconds(80))),
					Failed: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "failed", "running"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "failed", "running"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "failed", "running"}: 8,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "failed", "running"}: 8,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "failed", "running", "cpu"}: 8 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "failed", "running", "cpu"}: 8 * 16,
			},
		},
		"Preempted": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(60)).
								WithPendingTime(baseTimePlusSeconds(62)).
								WithRunningTime(baseTimePlusSeconds(72)).
								WithPreemptedTime(baseTimePlusSeconds(80))),
					Preempted: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "preempted", "running"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "preempted", "running"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "preempted", "running"}: 8,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "preempted", "running"}: 8,
			},
			expectedJobStateResourceSecondsByQueue: map[[5]string]float64{
				{testQueue, testPool, "preempted", "running", "cpu"}: 8 * 16,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "preempted", "running", "cpu"}: 8 * 16,
			},
		},
		"Multiple transitions": {
			trackedResourceNames: []v1.ResourceName{"cpu"},
			jsts: []jobdb.JobStateTransitions{
				{
					Job: baseJob.
						WithUpdatedRun(
							baseRun.
								WithLeasedTime(baseTimePlusSeconds(1)).
								WithPendingTime(baseTimePlusSeconds(3)).
								WithRunningTime(baseTimePlusSeconds(6)).
								WithTerminatedTime(baseTimePlusSeconds(10))),
					Leased:    true,
					Pending:   true,
					Running:   true,
					Succeeded: true,
				},
			},
			expectedJobStateCounterByQueue: map[[4]string]float64{
				{testQueue, testPool, "pending", "leased"}:    1,
				{testQueue, testPool, "running", "pending"}:   1,
				{testQueue, testPool, "succeeded", "running"}: 1,
			},
			expectedJobStateCounterByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "pending", "leased"}:    1,
				{testNode, testPool, testCluster, "running", "pending"}:   1,
				{testNode, testPool, testCluster, "succeeded", "running"}: 1,
			},
			expectedJobStateSecondsByQueue: map[[4]string]float64{
				{testQueue, testPool, "pending", "leased"}:    2,
				{testQueue, testPool, "running", "pending"}:   3,
				{testQueue, testPool, "succeeded", "running"}: 4,
			},
			expectedJobStateSecondsByNode: map[[5]string]float64{
				{testNode, testPool, testCluster, "pending", "leased"}:    2,
				{testNode, testPool, testCluster, "running", "pending"}:   3,
				{testNode, testPool, testCluster, "succeeded", "running"}: 4,
			},
			expectedJobStateResourceSecondsByNode: map[[6]string]float64{
				{testNode, testPool, testCluster, "pending", "leased", "cpu"}:    32,
				{testNode, testPool, testCluster, "running", "pending", "cpu"}:   48,
				{testNode, testPool, testCluster, "succeeded", "running", "cpu"}: 64,
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			metrics := newJobStateMetrics(tc.errorRegexes, tc.trackedResourceNames, []time.Duration{}, 12*time.Hour)
			metrics.ReportStateTransitions(tc.jsts, tc.jobRunErrorsByRunId)

			// jobStateCounterByQueue
			for k, v := range tc.expectedJobStateCounterByQueue {
				actualCounter := testutil.ToFloat64(metrics.jobStateCounterByQueue.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualCounter, epsilon, "jobStateCounterByQueue for %s", strings.Join(k[:], ","))
			}

			// jobStateCounterByQueue
			for k, v := range tc.expectedJobStateCounterByNode {
				actualCounter := testutil.ToFloat64(metrics.jobStateCounterByNode.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualCounter, epsilon, "jobStateCounterByNode for %s", strings.Join(k[:], ","))
			}

			// jobStateSecondsByNode
			for k, v := range tc.expectedJobStateSecondsByNode {
				actualJobStateSeconds := testutil.ToFloat64(metrics.jobStateSecondsByNode.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateSecondsByNode for %s", strings.Join(k[:], ","))
			}

			// jobStateSecondsByQueue
			for k, v := range tc.expectedJobStateSecondsByQueue {
				actualJobStateSeconds := testutil.ToFloat64(metrics.jobStateSecondsByQueue.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateSecondsByQueue for %s", strings.Join(k[:], ","))
			}

			// jobStateSecondsByNode
			for k, v := range tc.expectedJobStateSecondsByNode {
				actualJobStateSeconds := testutil.ToFloat64(metrics.jobStateSecondsByNode.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateSecondsByNode for %s", strings.Join(k[:], ","))
			}

			// jobStateResourceSecondsByQueue
			for k, v := range tc.expectedJobStateResourceSecondsByQueue {
				actualJobStateSeconds := testutil.ToFloat64(metrics.jobStateResourceSecondsByQueue.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateResourceSecondsByQueue for %s", strings.Join(k[:], ","))
			}

			// jobStateResourceSecondsByNode
			for k, v := range tc.expectedJobStateResourceSecondsByNode {
				actualJobStateSeconds := testutil.ToFloat64(metrics.jobStateResourceSecondsByNode.WithLabelValues(k[:]...))
				assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobStateResourceSecondsByNode for %s", strings.Join(k[:], ","))
			}
		})
	}
}

func TestReportJobPreempted(t *testing.T) {
	baseTimePlusSeconds := func(numSeconds int) *time.Time {
		newTime := baseTime.Add(time.Duration(numSeconds) * time.Second)
		return &newTime
	}

	// 60 second runtime
	job := baseJob.
		WithUpdatedRun(
			baseRun.
				WithLeasedTime(baseTimePlusSeconds(55)).
				WithPendingTime(baseTimePlusSeconds(57)).
				WithRunningTime(baseTimePlusSeconds(60)).
				WithPreemptedTime(baseTimePlusSeconds(120)))

	jobCheckpointIntervals := []time.Duration{time.Second * 5, time.Second * 30, time.Minute * 5}

	metrics := newJobStateMetrics(nil, []v1.ResourceName{"cpu"}, jobCheckpointIntervals, 12*time.Hour)
	metrics.ReportJobPreempted(job)

	expectedJobResourceSecondsLostToPreemptionByQueue := map[[4]string]float64{
		{testQueue, testPool, noCheckpointLabelValue, "cpu"}: 60 * 16,
		{testQueue, testPool, "5s", "cpu"}:                   5 * 16,
		{testQueue, testPool, "30s", "cpu"}:                  30 * 16,
		{testQueue, testPool, "5m", "cpu"}:                   60 * 16,
	}
	for k, v := range expectedJobResourceSecondsLostToPreemptionByQueue {
		actualJobStateSeconds := testutil.ToFloat64(metrics.jobResourceSecondsLostToPreemptionByQueue.WithLabelValues(k[:]...))
		assert.InDelta(t, v, actualJobStateSeconds, epsilon, "jobResourceSecondsLostToPreemptionByQueue for %s", strings.Join(k[:], ","))
	}
}

func TestCategoriseErrors(t *testing.T) {
	run := baseRun.
		WithExecutor(testCluster).
		WithNodeName(testNode).
		WithPool(testPool)

	job := baseJob.WithUpdatedRun(run)

	r, err := regexp.Compile("generic pod error")
	require.NoError(t, err)

	jobRunErrorsByRunId := map[string]*armadaevents.Error{
		run.Id(): {
			Terminal: true,
			Reason: &armadaevents.Error_PodError{
				PodError: &armadaevents.PodError{
					Message: "generic pod error",
				},
			},
		},
	}

	jsts := []jobdb.JobStateTransitions{
		{
			Job:    job,
			Failed: true,
		},
	}

	metrics := newJobStateMetrics([]*regexp.Regexp{r}, []v1.ResourceName{"cpu"}, []time.Duration{}, 12*time.Hour)
	metrics.ReportStateTransitions(jsts, jobRunErrorsByRunId)

	actualjobErrorsByQueue := testutil.ToFloat64(metrics.jobErrorsByQueue.WithLabelValues(testQueue, testPool, "podError", "generic pod error"))
	assert.InDelta(t, 1, actualjobErrorsByQueue, epsilon)

	actualjobErrorsByNode := testutil.ToFloat64(metrics.jobErrorsByNode.WithLabelValues(testNode, testPool, testCluster, "podError", "generic pod error"))
	assert.InDelta(t, 1, actualjobErrorsByNode, epsilon)
}

func TestReset(t *testing.T) {
	byQueueLabels := []string{testQueue, testPool}
	byQueueAndStateLabels := append(byQueueLabels, "running", "pending")
	byNodeLabels := []string{testNode, testPool, testCluster, "running", "pending"}
	byQueueResourceLabels := append(byQueueAndStateLabels, "cpu")
	byNodeResourceLabels := append(byNodeLabels, "cpu")
	resourceSecondsLostToPreemptionLabels := append(byQueueLabels, "none", "cpu")
	m := newJobStateMetrics(nil, nil, []time.Duration{}, 12*time.Hour)

	testReset := func(vec *prometheus.CounterVec, labels []string) {
		vec.WithLabelValues(labels...).Inc()
		counterVal := testutil.ToFloat64(vec.WithLabelValues(labels...))
		assert.Equal(t, 1.0, counterVal)
		m.reset()
		counterVal = testutil.ToFloat64(vec.WithLabelValues(labels...))
		assert.Equal(t, 0.0, counterVal)
	}

	testReset(m.jobStateCounterByQueue, byQueueAndStateLabels)
	testReset(m.jobStateSecondsByNode, byNodeLabels)
	testReset(m.jobStateSecondsByQueue, byQueueAndStateLabels)
	testReset(m.jobStateSecondsByNode, byNodeLabels)
	testReset(m.jobStateResourceSecondsByQueue, byQueueResourceLabels)
	testReset(m.jobStateResourceSecondsByNode, byNodeResourceLabels)
	testReset(m.jobErrorsByQueue, byQueueAndStateLabels)
	testReset(m.jobErrorsByNode, byNodeLabels)
	testReset(m.jobResourceSecondsLostToPreemptionByQueue, resourceSecondsLostToPreemptionLabels)
}

func TestDisable(t *testing.T) {
	byQueueLabels := []string{testQueue, testPool}
	byQueueAndStateLabels := append(byQueueLabels, "running", "pending")
	byNodeLabels := []string{testNode, testPool, testCluster, "running", "pending"}
	byQueueResourceLabels := append(byQueueAndStateLabels, "cpu")
	byNodeResourceLabels := append(byNodeLabels, "cpu")
	resourceSecondsLostToPreemptionLabels := append(byQueueLabels, "none", "cpu")

	collect := func(m *jobStateMetrics) []prometheus.Metric {
		m.jobStateCounterByQueue.WithLabelValues(byQueueAndStateLabels...).Inc()
		m.jobStateSecondsByNode.WithLabelValues(byNodeLabels...).Inc()
		m.jobStateSecondsByQueue.WithLabelValues(byQueueAndStateLabels...).Inc()
		m.jobStateSecondsByNode.WithLabelValues(byNodeLabels...).Inc()
		m.jobStateResourceSecondsByQueue.WithLabelValues(byQueueResourceLabels...).Inc()
		m.jobStateResourceSecondsByNode.WithLabelValues(byNodeResourceLabels...).Inc()
		m.jobErrorsByQueue.WithLabelValues(byQueueAndStateLabels...).Inc()
		m.jobErrorsByNode.WithLabelValues(byNodeLabels...).Inc()
		m.jobResourceSecondsLostToPreemptionByQueue.WithLabelValues(resourceSecondsLostToPreemptionLabels...).Inc()

		ch := make(chan prometheus.Metric, 1000)
		m.collect(ch)
		collected := make([]prometheus.Metric, 0, len(ch))
		for len(ch) > 0 {
			collected = append(collected, <-ch)
		}
		return collected
	}

	m := newJobStateMetrics(nil, nil, []time.Duration{}, 12*time.Hour)

	// Enabled
	assert.NotZero(t, len(collect(m)))

	// Disabled
	m.disable()
	assert.Zero(t, len(collect(m)))

	// Enabled
	m.enable()
	assert.NotZero(t, len(collect(m)))
}
