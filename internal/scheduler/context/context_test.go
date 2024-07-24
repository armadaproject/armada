package context

import (
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

func TestNewGangSchedulingContext(t *testing.T) {
	jctxs := testNSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass, 2)
	gctx := NewGangSchedulingContext(jctxs)
	assert.Equal(t, jctxs, gctx.JobSchedulingContexts)
	assert.Equal(t, "A", gctx.Queue)
	assert.Equal(t, testfixtures.TestDefaultPriorityClass, gctx.GangInfo.PriorityClassName)
	assert.True(
		t,
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("8Gi"),
			},
		}.Equal(
			gctx.TotalResourceRequests,
		),
	)
}

func TestSchedulingContextAccounting(t *testing.T) {
	totalResources := schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}}
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalResources, []string{"cpu"})
	require.NoError(t, err)
	sctx := NewSchedulingContext(
		"executor",
		"pool",
		testfixtures.TestPriorityClasses,
		testfixtures.TestDefaultPriorityClass,
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
		err := sctx.AddQueueSchedulingContext(queue, priorityFactorByQueue[queue], allocatedByQueueAndPriorityClass[queue], schedulerobjects.ResourceList{}, nil)
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

func TestJobSchedulingContext_SetAssignedNodeId(t *testing.T) {
	jctx := &JobSchedulingContext{}

	assert.Equal(t, "", jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	// Will not add a node selector if input is empty
	jctx.SetAssignedNodeId("")
	assert.Equal(t, "", jctx.GetAssignedNodeId())
	assert.Empty(t, jctx.AdditionalNodeSelectors)

	jctx.SetAssignedNodeId("node1")
	assert.Equal(t, "node1", jctx.GetAssignedNodeId())
	assert.Len(t, jctx.AdditionalNodeSelectors, 1)
	assert.Equal(t, map[string]string{configuration.NodeIdLabel: "node1"}, jctx.AdditionalNodeSelectors)
}

func TestCalculateFairShares(t *testing.T) {
	zeroCpu := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse("0")},
	}
	oneCpu := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")},
	}
	fortyCpu := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse("40")},
	}
	oneHundredCpu := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse("100")},
	}
	oneThousandCpu := schedulerobjects.ResourceList{
		Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1000")},
	}
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
			fairnessCostProvider, err := fairness.NewDominantResourceFairness(tc.availableResources, []string{"cpu"})
			require.NoError(t, err)
			sctx := NewSchedulingContext(
				"executor",
				"pool",
				testfixtures.TestPriorityClasses,
				testfixtures.TestDefaultPriorityClass,
				fairnessCostProvider,
				nil,
				tc.availableResources,
			)
			for qName, q := range tc.queueCtxs {
				err = sctx.AddQueueSchedulingContext(qName, q.Weight, schedulerobjects.QuantityByTAndResourceType[string]{}, q.Demand, nil)
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
