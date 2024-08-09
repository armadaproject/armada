package context

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestSchedulingContextAccounting(t *testing.T) {
	totalResources := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"cpu"}})
	require.NoError(t, err)
	sctx := NewSchedulingContext(
		"pool",
		fairnessCostProvider,
		nil,
		totalResources,
	)
	priorityFactorByQueue := map[string]float64{"A": 1, "B": 1}
	allocatedByQueueAndPriorityClass := map[string]schedulerobjects.QuantityByTAndResourceType[string]{
		"A": {
			"foo": schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
		},
	}
	for _, queue := range []string{"A", "B"} {
		err := sctx.AddQueueSchedulingContext(queue, priorityFactorByQueue[queue], allocatedByQueueAndPriorityClass[queue], schedulerobjects.ResourceList{}, schedulerobjects.ResourceList{}, nil)
		require.NoError(t, err)
	}

	expected := sctx.AllocatedByQueueAndPriority()
	jctxs := testNSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass, 2)
	gctx := NewGangSchedulingContext(jctxs)
	_, err = sctx.AddGangSchedulingContext(gctx)
	require.NoError(t, err)
	for _, jctx := range jctxs {
		_, err := sctx.EvictJob(jctx.Job)
		require.NoError(t, err)
	}

	actual := sctx.AllocatedByQueueAndPriority()
	queues := armadaslices.Unique(
		armadaslices.Concatenate(maps.Keys(actual), maps.Keys(expected)),
	)
	for _, queue := range queues {
		assert.True(t, expected[queue].Equal(actual[queue]))
	}
	_, err = sctx.AddGangSchedulingContext(gctx)
	require.NoError(t, err)
}

func TestCalculateFairShares(t *testing.T) {
	zeroCpu := nCpu(0)
	oneCpu := nCpu(1)
	fortyCpu := nCpu(40)
	oneHundredCpu := nCpu(100)
	oneThousandCpu := nCpu(1000)
	tests := map[string]struct {
		availableResources         schedulerobjects.ResourceList
		queueCtxs                  map[string]*QueueSchedulingContext
		expectedFairShares         map[string]float64
		expectedAdjustedFairShares map[string]float64
	}{
		"one queue, demand exceeds capacity": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 1.0},
			expectedAdjustedFairShares: map[string]float64{"queueA": 1.0},
		},
		"one queue, demand less than capacity": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 1.0},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.01},
		},
		"two queues, equal weights, demand exceeds capacity": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneThousandCpu},
				"queueB": {Weight: 1.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.5, "queueB": 0.5},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.5, "queueB": 0.5},
		},
		"two queues, equal weights, demand less than capacity for both queues": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
				"queueB": {Weight: 1.0, Demand: oneCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.5, "queueB": 0.5},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.01, "queueB": 0.01},
		},
		"two queues, equal weights, demand less than capacity for one queue": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
				"queueB": {Weight: 1.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.5, "queueB": 0.5},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.01, "queueB": 0.99},
		},
		"two queues, non equal weights, demand exceeds capacity for both queues": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneThousandCpu},
				"queueB": {Weight: 3.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.25, "queueB": 0.75},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.25, "queueB": 0.75},
		},
		"two queues, non equal weights, demand exceeds capacity for higher priority queue only": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
				"queueB": {Weight: 3.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.25, "queueB": 0.75},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.01, "queueB": 0.99},
		},
		"two queues, non equal weights, demand exceeds capacity for lower priority queue only": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneThousandCpu},
				"queueB": {Weight: 3.0, Demand: oneCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 0.25, "queueB": 0.75},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.99, "queueB": 0.01},
		},
		"three queues, equal weights. Adjusted fair share requires multiple iterations": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
				"queueB": {Weight: 1.0, Demand: fortyCpu},
				"queueC": {Weight: 1.0, Demand: oneThousandCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 1.0 / 3, "queueB": 1.0 / 3, "queueC": 1.0 / 3},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.01, "queueB": 0.4, "queueC": 0.59},
		},
		"No demand": {
			availableResources: oneHundredCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: zeroCpu},
				"queueB": {Weight: 1.0, Demand: zeroCpu},
				"queueC": {Weight: 1.0, Demand: zeroCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 1.0 / 3, "queueB": 1.0 / 3, "queueC": 1.0 / 3},
			expectedAdjustedFairShares: map[string]float64{"queueA": 0.0, "queueB": 0.0, "queueC": 0.0},
		},
		"No capacity": {
			availableResources: zeroCpu,
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Weight: 1.0, Demand: oneCpu},
				"queueB": {Weight: 1.0, Demand: oneCpu},
				"queueC": {Weight: 1.0, Demand: oneCpu},
			},
			expectedFairShares:         map[string]float64{"queueA": 1.0 / 3, "queueB": 1.0 / 3, "queueC": 1.0 / 3},
			expectedAdjustedFairShares: map[string]float64{"queueA": 1.0 / 3, "queueB": 1.0 / 3, "queueC": 1.0 / 3},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fairnessCostProvider, err := fairness.NewDominantResourceFairness(tc.availableResources, configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"cpu"}})
			require.NoError(t, err)
			sctx := NewSchedulingContext(
				"pool",
				fairnessCostProvider,
				nil,
				tc.availableResources,
			)
			for qName, q := range tc.queueCtxs {
				err = sctx.AddQueueSchedulingContext(
					qName, q.Weight, schedulerobjects.QuantityByTAndResourceType[string]{}, q.Demand, q.Demand, nil)
				require.NoError(t, err)
			}
			sctx.UpdateFairShares()
			for qName, qctx := range sctx.QueueSchedulingContexts {
				expectedFairShare, ok := tc.expectedFairShares[qName]
				require.True(t, ok, "Expected fair share for queue %s not found", qName)
				expectedAdjustedFairShare, ok := tc.expectedAdjustedFairShares[qName]
				require.True(t, ok, "Expected adjusted fair share for queue %s not found", qName)
				assert.Equal(t, expectedFairShare, qctx.FairShare, "Fair share for queue %s", qName)
				assert.Equal(t, expectedAdjustedFairShare, qctx.AdjustedFairShare, "Adjusted Fair share for queue %s", qName)
			}
		})
	}
}

func TestCalculateFairnessError(t *testing.T) {
	tests := map[string]struct {
		availableResources schedulerobjects.ResourceList
		queueCtxs          map[string]*QueueSchedulingContext
		expected           float64
	}{
		"one queue, no error": {
			availableResources: nCpu(100),
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Allocated: nCpu(50), AdjustedFairShare: 0.5},
			},
			expected: 0,
		},
		"two queues, no error": {
			availableResources: nCpu(100),
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Allocated: nCpu(50), AdjustedFairShare: 0.5},
				"queueB": {Allocated: nCpu(50), AdjustedFairShare: 0.5},
			},
			expected: 0,
		},
		"one queue with error": {
			availableResources: nCpu(100),
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Allocated: nCpu(40), AdjustedFairShare: 0.5},
			},
			expected: 0.1,
		},
		"two queues with error": {
			availableResources: nCpu(100),
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Allocated: nCpu(40), AdjustedFairShare: 0.5},
				"queueB": {Allocated: nCpu(10), AdjustedFairShare: 0.5},
			},
			expected: 0.5,
		},
		"above fair share is not counted": {
			availableResources: nCpu(100),
			queueCtxs: map[string]*QueueSchedulingContext{
				"queueA": {Allocated: nCpu(100), AdjustedFairShare: 0.5},
			},
			expected: 0.0,
		},
		"empty": {
			availableResources: nCpu(100),
			queueCtxs:          map[string]*QueueSchedulingContext{},
			expected:           0.0,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			fairnessCostProvider, err := fairness.NewDominantResourceFairness(tc.availableResources, configuration.SchedulingConfig{DominantResourceFairnessResourcesToConsider: []string{"cpu"}})
			require.NoError(t, err)
			sctx := NewSchedulingContext("pool", fairnessCostProvider, nil, tc.availableResources)
			sctx.QueueSchedulingContexts = tc.queueCtxs
			assert.InDelta(t, tc.expected, sctx.FairnessError(), 0.00001)
		})
	}
}

func testNSmallCpuJobSchedulingContext(queue, priorityClassName string, n int) []*JobSchedulingContext {
	rv := make([]*JobSchedulingContext, n)
	for i := 0; i < n; i++ {
		rv[i] = testSmallCpuJobSchedulingContext(queue, priorityClassName)
	}
	return rv
}

func testSmallCpuJobSchedulingContext(queue, priorityClassName string) *JobSchedulingContext {
	job := testfixtures.Test1Cpu4GiJob(queue, priorityClassName)
	return &JobSchedulingContext{
		JobId:                job.Id(),
		Job:                  job,
		PodRequirements:      job.PodRequirements(),
		ResourceRequirements: job.EfficientResourceRequirements(),
		GangInfo:             EmptyGangInfo(job),
	}
}

func nCpu(n int) schedulerobjects.ResourceList {
	return schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse(fmt.Sprintf("%d", n))},
	}
}
