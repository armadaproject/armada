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

	ok, err := MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"armada/region": "eu"}},
		{Labels: map[string]string{"armada/zone": "2"}},
	}})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
	}})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
		{Labels: map[string]string{"x": "y"}},
		{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
	}})
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_MatchSchedulingRequirements_isAbleToFitOnAvailableNodes(t *testing.T) {
	request := v1.ResourceList{"cpu": resource.MustParse("2"), "memory": resource.MustParse("2Gi")}
	resourceRequirement := v1.ResourceRequirements{
		Limits:   request,
		Requests: request,
	}
	job := &api.Job{PodSpec: &v1.PodSpec{Containers: []v1.Container{{Resources: resourceRequirement}}}}

	ok, err := MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{
		NodeTypes: []*api.NodeType{{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}}},
	})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{
		NodeTypes: []*api.NodeType{
			{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
		},
	})
	assert.True(t, ok)
	assert.NoError(t, err)
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
			nodeType: api.NodeType{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 4, "memory": 4 * 1024 * 1024 * 1024},
		},
		{
			nodeType: api.NodeType{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
		},
	}, aggregated)
}

func Test_AggregateNodeTypesAllocations_NodesWithMoreTaintsGoFirst(t *testing.T) {
	nodes := []api.NodeInfo{
		{
			Name:                 "n1",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
			Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}},
		},
		{
			Name:                 "n2",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
			Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}, {Key: "two", Value: "2", Effect: "NoSchedule"}},
		},
	}

	aggregated := AggregateNodeTypeAllocations(nodes)
	assert.Equal(t, []*nodeTypeAllocation{
		{
			nodeType: api.NodeType{
				Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}, {Key: "two", Value: "2", Effect: "NoSchedule"}},
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
		},
		{
			nodeType: api.NodeType{
				Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}},
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 2, "memory": 1 * 1024 * 1024 * 1024},
		},
	}, aggregated)
}
