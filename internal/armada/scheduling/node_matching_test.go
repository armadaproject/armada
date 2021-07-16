package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

func Test_MatchSchedulingRequirements_labels(t *testing.T) {
	job := &api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"armada/region": "eu", "armada/zone": "1"}}}

	assert.False(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{}))
	assert.False(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"armada/region": "eu"}},
		{Labels: map[string]string{"armada/zone": "2"}},
	}}))
	assert.False(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
	}}))

	assert.True(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"x": "y"}},
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
	}}))
}

func Test_MatchSchedulingRequirements_isAbleToFitOnAvailableNodes(t *testing.T) {
	request := v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	resourceRequirement := v1.ResourceRequirements{
		Limits:   request,
		Requests: request,
	}
	job := &api.Job{PodSpec: &v1.PodSpec{Containers: []v1.Container{{Resources: resourceRequirement}}}}

	assert.False(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{}))

	assert.False(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{
		NodeTypes: []*api.NodeType{{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}},
	}))

	assert.True(t, MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{
		NodeTypes: []*api.NodeType{
			{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
		},
	}))
}

func Test_AggregateNodeTypesAllocations(t *testing.T) {

	nodes := []api.NodeInfo{
		{
			Name:                 "n1",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
		},
		{
			Name:                 "n2",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("3Gi")},
		},
		{
			Name:                 "n3-special",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
		},
	}

	aggregated := AggregateNodeTypeAllocations(nodes)
	assert.Equal(t, []*nodeTypeAllocation{
		{
			taints:             nil,
			labels:             nil,
			nodeSize:           common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			availableResources: common.ComputeResourcesFloat{"cpu": 4, "memory": 4 * 1024 * 1024 * 1024},
			nodeCount:          2,
		},
		{
			taints:             nil,
			labels:             nil,
			nodeSize:           common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			availableResources: common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
			nodeCount:          1,
		},
	}, aggregated)
}

func Test_fits(t *testing.T) {
	available := makeResourceList(1, 10).AsFloat()

	assert.False(t, fits(makeResourceList(2, 20).AsFloat(), available))
	assert.False(t, fits(makeResourceList(2, 5).AsFloat(), available))
	assert.True(t, fits(makeResourceList(1, 10).AsFloat(), available))
}

func Test_matchNodeSelector(t *testing.T) {
	labels := map[string]string{
		"A": "test",
		"B": "test",
	}
	assert.False(t, matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"C": "test"}}, labels))
	assert.False(t, matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"B": "42"}}, labels))
	assert.True(t, matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"A": "test"}}, labels))
	assert.True(t, matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"A": "test", "B": "test"}}, labels))
}

func Test_tolerates(t *testing.T) {
	taints := []v1.Taint{
		{
			Key:    "A",
			Value:  "test",
			Effect: v1.TaintEffectNoSchedule,
		},
		{
			Key:    "B",
			Value:  "test",
			Effect: v1.TaintEffectPreferNoSchedule,
		},
	}

	podSpec := &v1.PodSpec{
		Tolerations: []v1.Toleration{
			{
				Key:      "A",
				Operator: v1.TolerationOpEqual,
				Value:    "test",
				Effect:   v1.TaintEffectNoSchedule,
			},
		}}

	assert.False(t, tolerates(&v1.PodSpec{}, taints))
	assert.True(t, tolerates(podSpec, taints))
}

func Test_roundString(t *testing.T) {
	assert.Equal(t, "100e-2", roundQuantityString(resource.MustParse("1"), 3))
	assert.Equal(t, "100e-5", roundQuantityString(resource.MustParse("1m"), 3))
	assert.Equal(t, "137e9", roundQuantityString(resource.MustParse("128Gi"), 3))
	assert.Equal(t, "128e6", roundQuantityString(resource.MustParse("128M"), 3))
}

func makeResourceList(cores int64, gigabytesRam int64) common.ComputeResources {
	cpuResource := resource.NewQuantity(cores, resource.DecimalSI)
	memoryResource := resource.NewQuantity(gigabytesRam*1024*1024*1024, resource.DecimalSI)
	resourceMap := common.ComputeResources{
		string(v1.ResourceCPU):    *cpuResource,
		string(v1.ResourceMemory): *memoryResource,
	}
	return resourceMap
}
