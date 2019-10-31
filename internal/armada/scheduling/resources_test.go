package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
)

// 1 cpu per 1 Gb
var scarcity = map[string]float64{"cpu": 1, "memory": 1.0 / (1024 * 1024 * 1024)}

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

	slices := SliceResource(scarcity, queuePriorities, common.ComputeResources{"cpu": resource.MustParse("8")})

	// resulted usage ration should be 4 : 4 : 4
	twoCpu := common.ComputeResourcesFloat{"cpu": 2.0}
	fourCpu := common.ComputeResourcesFloat{"cpu": 4.0}
	assert.Equal(t, slices, map[*api.Queue]common.ComputeResourcesFloat{q1: twoCpu, q2: twoCpu, q3: fourCpu})
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

	slices := SliceResource(scarcity, queuePriorities, common.ComputeResources{"cpu": resource.MustParse("3")})

	noCpu := common.ComputeResourcesFloat{"cpu": 0.0}
	allCpu := common.ComputeResourcesFloat{"cpu": 3.0}
	assert.Equal(t, slices, map[*api.Queue]common.ComputeResourcesFloat{q1: noCpu, q2: allCpu})
}
