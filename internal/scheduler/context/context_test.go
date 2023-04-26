package context

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
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

func testNSmallCpuJobSchedulingContext(queue, priorityClassName string, n int) []*JobSchedulingContext {
	rv := make([]*JobSchedulingContext, n)
	for i := 0; i < n; i++ {
		rv[i] = testSmallCpuJobSchedulingContext(queue, priorityClassName)
	}
	return rv
}

func testSmallCpuJobSchedulingContext(queue, priorityClassName string) *JobSchedulingContext {
	job := testfixtures.SmallCpuJob(queue, priorityClassName)
	return &JobSchedulingContext{
		ExecutorId: "executor",
		NumNodes:   1,
		JobId:      job.GetId(),
		Job:        job,
		Req:        job.GetRequirements(nil).ObjectRequirements[0].GetPodRequirements(),
	}
}

func TestSchedulingContextAccounting(t *testing.T) {
	sctx := NewSchedulingContext(
		"executor",
		"pool",
		testfixtures.TestPriorityClasses,
		testfixtures.TestDefaultPriorityClass,
		map[string]float64{"cpu": 1},
		map[string]float64{"A": 1, "B": 1},
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
		map[string]schedulerobjects.QuantityByPriorityAndResourceType{
			"A": {
				0: schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1")}},
			},
		},
	)
	expected := sctx.AllocatedByQueueAndPriority()
	jctxs := testNSmallCpuJobSchedulingContext("A", testfixtures.TestDefaultPriorityClass, 2)
	gctx := NewGangSchedulingContext(jctxs)
	sctx.AddGangSchedulingContext(gctx)
	for _, jctx := range jctxs {
		sctx.EvictJob(jctx.Job)
	}

	actual := sctx.AllocatedByQueueAndPriority()
	queues := armadaslices.Unique(
		armadaslices.Concatenate(maps.Keys(actual), maps.Keys(expected)),
	)
	for _, queue := range queues {
		assert.True(t, expected[queue].Equal(actual[queue]))
	}
	sctx.AddGangSchedulingContext(gctx)
}
