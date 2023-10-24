package context

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
)

func TestNewGangSchedulingContext(t *testing.T) {
	jctxs := testNSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass, 2)
	gctx := NewGangSchedulingContext(jctxs)
	assert.Equal(t, jctxs, gctx.JobSchedulingContexts)
	assert.Equal(t, "A", gctx.Queue)
	assert.Equal(t, testfixtures.TestDefaultPriorityClass, gctx.PriorityClassName)
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
	fairnessCostProvider, err := fairness.NewAssetFairness(map[string]float64{"cpu": 1})
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
		err := sctx.AddQueueSchedulingContext(queue, priorityFactorByQueue[queue], allocatedByQueueAndPriorityClass[queue], nil)
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
		JobId:              job.GetId(),
		Job:                job,
		PodRequirements:    job.GetPodRequirements(testfixtures.TestPriorityClasses),
		GangMinCardinality: 1,
	}
}
