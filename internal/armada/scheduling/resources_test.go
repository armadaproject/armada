package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

// 1 cpu per 1 Gb
var scarcity = map[string]float64{"cpu": 1, "memory": 1.0 / (1024 * 1024 * 1024)}

type queueResources map[*api.Queue]common.ComputeResourcesFloat

// Verify that two maps of queues to compute-resources are basically equal, in
// keys, as well as resource cpu value, accepting a small tolerance for CPU
// resource, as some versions of Docker will report a microscopically
// fractional difference, instead of the exact smooth integer-like value that
// tests often specify.
func assertCpuResourceSoftEqual(t *testing.T, expected, actual queueResources) {
	assert.Equal(t, len(expected), len(actual))

	expectQs := make([]api.Queue, len(expected))
	for q := range expected {
		expectQs = append(expectQs, *q)
	}
	actualQs := make([]api.Queue, len(actual))
	for q := range actual {
		actualQs = append(actualQs, *q)
	}
	assert.ElementsMatch(t, expectQs, actualQs)

	for q, _ := range expected {
		actualRsrc, exists := actual[q]
		assert.True(t, exists)
		assert.NotNil(t, actualRsrc)

		assert.InDelta(t, expected[q]["cpu"], actualRsrc["cpu"], 0.001)
	}
}

func Test_sliceResources(t *testing.T) {

	q1 := &api.Queue{Name: "q1"}
	q2 := &api.Queue{Name: "q2"}
	q3 := &api.Queue{Name: "q3"}

	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	noResources := common.ComputeResources{}

	queuePriorities := map[*api.Queue]QueuePriorityInfo{
		q1: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q2: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q3: {Priority: 1, CurrentUsage: noResources},  // queue usage is 0
	}

	slices := sliceResource(scarcity, queuePriorities, common.ComputeResources{"cpu": resource.MustParse("8")}.AsFloat())

	// resulted usage ration should be 4 : 4 : 4
	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	fourCpu := common.ComputeResourcesFloat{"cpu": 4.0}
	expectRsrcs := map[*api.Queue]common.ComputeResourcesFloat{q1: twoCpu, q2: twoCpu, q3: fourCpu}

	assertCpuResourceSoftEqual(t, expectRsrcs, slices)
}

func Test_sliceResources_highImbalance(t *testing.T) {

	q1 := &api.Queue{Name: "q1"}
	q2 := &api.Queue{Name: "q2"}

	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("10"), "memory": resource.MustParse("10Gi")}
	noResources := common.ComputeResources{}

	queuePriorities := map[*api.Queue]QueuePriorityInfo{
		q1: {Priority: 1, CurrentUsage: cpuAndMemory},
		q2: {Priority: 1, CurrentUsage: noResources},
	}

	slices := sliceResource(scarcity, queuePriorities, common.ComputeResources{"cpu": resource.MustParse("3")}.AsFloat())

	noCpu := common.ComputeResourcesFloat{"cpu": 0.0}
	allCpu := common.ComputeResourcesFloat{"cpu": 3.0}
	assert.Equal(t, slices, map[*api.Queue]common.ComputeResourcesFloat{q1: noCpu, q2: allCpu})
}

func Test_SliceResourceWithLimits_SchedulingShareMatchesAdjusted_WhenNoQueuesAtLimit(t *testing.T) {

	q1 := &api.Queue{Name: "q1"}
	q2 := &api.Queue{Name: "q2"}
	q3 := &api.Queue{Name: "q3"}

	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}
	noResources := common.ComputeResources{}

	queuePriorities := map[*api.Queue]QueuePriorityInfo{
		q1: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q2: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q3: {Priority: 1, CurrentUsage: noResources},  // queue usage is 0
	}

	resourceToSlice := common.ComputeResources{"cpu": resource.MustParse("8")}.AsFloat()

	queueSchedulingInfo := map[*api.Queue]*QueueSchedulingInfo{
		q1: {remainingSchedulingLimit: resourceToSlice, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
		q2: {remainingSchedulingLimit: resourceToSlice, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
		q3: {remainingSchedulingLimit: resourceToSlice, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
	}

	slices := SliceResourceWithLimits(scarcity, queueSchedulingInfo, queuePriorities, resourceToSlice)

	// resulted usage ration should be 4 : 4 : 4
	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	fourCpu := common.ComputeResourcesFloat{"cpu": 4.0}

	assert.InDeltaMapValues(t, slices[q1].schedulingShare, twoCpu, 0.001)
	assert.InDeltaMapValues(t, slices[q1].adjustedShare, twoCpu, 0.001)
	assert.InDeltaMapValues(t, slices[q2].schedulingShare, twoCpu, 0.001)
	assert.InDeltaMapValues(t, slices[q2].adjustedShare, twoCpu, 0.001)
	assert.InDeltaMapValues(t, slices[q3].schedulingShare, fourCpu, 0.001)
	assert.InDeltaMapValues(t, slices[q3].adjustedShare, fourCpu, 0.001)
}

func Test_SliceResourceWithLimits_SchedulingShareCorrespondsWithPriority(t *testing.T) {
	q1 := &api.Queue{Name: "q1"}
	q2 := &api.Queue{Name: "q2"}

	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}

	queuePriorities := map[*api.Queue]QueuePriorityInfo{
		q1: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q2: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
	}

	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	fourCpu := common.ComputeResourcesFloat{"cpu": 4.0}
	resourceToSlice := common.ComputeResourcesFloat{"cpu": 8.0}

	queueSchedulingInfo := map[*api.Queue]*QueueSchedulingInfo{
		q1: {remainingSchedulingLimit: twoCpu, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
		q2: {remainingSchedulingLimit: resourceToSlice, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
	}

	slices := SliceResourceWithLimits(scarcity, queueSchedulingInfo, queuePriorities, resourceToSlice)

	//Both queues have the same priority so should have the same scheduling share
	assert.Equal(t, slices[q1].schedulingShare, fourCpu)
	assert.Equal(t, slices[q2].schedulingShare, fourCpu)
}

func Test_SliceResourceWithLimits_AdjustedShare(t *testing.T) {
	q1 := &api.Queue{Name: "q1"}
	q2 := &api.Queue{Name: "q2"}

	cpuAndMemory := common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}

	queuePriorities := map[*api.Queue]QueuePriorityInfo{
		q1: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
		q2: {Priority: 1, CurrentUsage: cpuAndMemory}, // queue usage is 2
	}

	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	fourCpu := common.ComputeResourcesFloat{"cpu": 4.0}
	resourceToSlice := common.ComputeResourcesFloat{"cpu": 8.0}

	queueSchedulingInfo := map[*api.Queue]*QueueSchedulingInfo{
		q1: {remainingSchedulingLimit: twoCpu, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
		q2: {remainingSchedulingLimit: resourceToSlice, schedulingShare: common.ComputeResourcesFloat{}, adjustedShare: common.ComputeResourcesFloat{}},
	}

	slices := SliceResourceWithLimits(scarcity, queueSchedulingInfo, queuePriorities, resourceToSlice)

	//Both queues have the same priority however q1 is limited to 2cpu
	assert.Equal(t, slices[q1].adjustedShare, twoCpu)
	assert.Equal(t, slices[q2].adjustedShare, fourCpu)
}

func TestQueueSchedulingInfo_UpdateLimits(t *testing.T) {
	oneCpu := common.ComputeResourcesFloat{"cpu": 1.0}
	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	data := NewQueueSchedulingInfo(twoCpu, twoCpu, twoCpu)
	data.UpdateLimits(oneCpu)

	assert.Equal(t, data.remainingSchedulingLimit, oneCpu)
	assert.Equal(t, data.schedulingShare, oneCpu)
	assert.Equal(t, data.adjustedShare, oneCpu)
}

func TestQueueSchedulingInfo_UpdateLimits_AdjustedLowerThanSchedulingShare(t *testing.T) {
	fiveCpu := common.ComputeResourcesFloat{"cpu": 5.0}
	tenCpu := common.ComputeResourcesFloat{"cpu": 10.0}
	data := NewQueueSchedulingInfo(tenCpu, tenCpu, fiveCpu)
	data.UpdateLimits(common.ComputeResourcesFloat{"cpu": 1.0})

	assert.Equal(t, data.remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 9.0})
	assert.Equal(t, data.schedulingShare, common.ComputeResourcesFloat{"cpu": 8.0})
	assert.Equal(t, data.adjustedShare, common.ComputeResourcesFloat{"cpu": 4.0})
}

func TestQueueSchedulingInfo_UpdateLimits_AdjustedHigherThanSchedulingShare(t *testing.T) {
	fiveCpu := common.ComputeResourcesFloat{"cpu": 5.0}
	tenCpu := common.ComputeResourcesFloat{"cpu": 10.0}
	data := NewQueueSchedulingInfo(tenCpu, fiveCpu, tenCpu)
	data.UpdateLimits(common.ComputeResourcesFloat{"cpu": 2.0})

	assert.Equal(t, data.remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 8.0})
	assert.Equal(t, data.schedulingShare, common.ComputeResourcesFloat{"cpu": 4.0})
	assert.Equal(t, data.adjustedShare, common.ComputeResourcesFloat{"cpu": 8.0})
}

func TestQueueSchedulingInfo_UpdateLimits_ValuesLimitedAt0(t *testing.T) {
	oneCpu := common.ComputeResourcesFloat{"cpu": 1.0}
	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	data := NewQueueSchedulingInfo(oneCpu, oneCpu, oneCpu)
	data.UpdateLimits(twoCpu)

	assert.Equal(t, data.remainingSchedulingLimit, common.ComputeResourcesFloat{"cpu": 0.0})
	assert.Equal(t, data.schedulingShare, common.ComputeResourcesFloat{"cpu": 0.0})
	assert.Equal(t, data.adjustedShare, common.ComputeResourcesFloat{"cpu": 0.0})
}
