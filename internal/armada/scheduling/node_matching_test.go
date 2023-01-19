package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
)

func Test_MatchSchedulingRequirements_labels(t *testing.T) {
	job := &api.Job{PodSpec: &v1.PodSpec{NodeSelector: map[string]string{"armada/region": "eu", "armada/zone": "1"}}}

	ok, err := MatchSchedulingRequirements(job, &api.ClusterSchedulingInfoReport{})
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(
		job,
		&api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
			{Labels: map[string]string{"armada/region": "eu"}},
			{Labels: map[string]string{"armada/zone": "2"}},
		}},
	)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(
		job,
		&api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
			{Labels: map[string]string{"armada/region": "eu", "armada/zone": "2"}},
		}},
	)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(
		job,
		&api.ClusterSchedulingInfoReport{NodeTypes: []*api.NodeType{
			{Labels: map[string]string{"x": "y"}},
			{Labels: map[string]string{"armada/region": "eu", "armada/zone": "1", "x": "y"}},
		}},
	)
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

	ok, err = MatchSchedulingRequirements(
		job,
		&api.ClusterSchedulingInfoReport{
			NodeTypes: []*api.NodeType{
				{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
			},
		},
	)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = MatchSchedulingRequirements(
		job,
		&api.ClusterSchedulingInfoReport{
			NodeTypes: []*api.NodeType{
				{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("1Gi")}},
				{AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("3"), "memory": resource.MustParse("3Gi")}},
			},
		},
	)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_AggregateNodeTypesAllocations(t *testing.T) {
	nodes := []api.NodeInfo{
		{
			Name:                 "n1",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
			TotalResources:       common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
			AllocatedResources: map[int32]api.ComputeResource{
				0: {
					Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1.2"), "memory": resource.MustParse("2.5Gi")},
				},
			},
		},
		{
			Name:                 "n2",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("3Gi")},
			TotalResources:       common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("3Gi")},
			AllocatedResources: map[int32]api.ComputeResource{
				0: {
					Resources: map[string]resource.Quantity{"cpu": resource.MustParse("0.8"), "memory": resource.MustParse("3.5Gi")},
				},
			},
		},
		{
			Name:                 "n3-special",
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
			TotalResources:       common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
			AllocatedResources: map[int32]api.ComputeResource{
				0: {
					Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
				},
			},
		},
	}

	aggregated := AggregateNodeTypeAllocations(nodes)
	expected := []*nodeTypeAllocation{
		{
			nodeType: api.NodeType{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 4, "memory": 4 * 1024 * 1024 * 1024},
			totalResources:     common.ComputeResourcesFloat{"cpu": 4, "memory": 4 * 1024 * 1024 * 1024},
			allocatedResources: map[int32]common.ComputeResourcesFloat{
				0: {"cpu": 2, "memory": 6 * 1024 * 1024 * 1024},
			},
		},
		{
			nodeType: api.NodeType{
				Taints:               nil,
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
			totalResources:     common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
			allocatedResources: map[int32]common.ComputeResourcesFloat{
				0: {"cpu": 1, "memory": 3 * 1024 * 1024 * 1024},
			},
		},
	}
	for i := range aggregated {
		assert.Equal(t, expected[i], aggregated[i])
	}
}

func Test_AggregateNodeTypesAllocations_NodesWithMoreTaintsGoFirst(t *testing.T) {
	nodes := []api.NodeInfo{
		{
			Name:                 "n1",
			Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}},
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
			TotalResources:       common.ComputeResources{"cpu": resource.MustParse("2"), "memory": resource.MustParse("1Gi")},
			AllocatedResources: map[int32]api.ComputeResource{
				0: {
					Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("2.5Gi")},
				},
			},
		},
		{
			Name:                 "n2",
			Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}, {Key: "two", Value: "2", Effect: "NoSchedule"}},
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			AvailableResources:   common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
			TotalResources:       common.ComputeResources{"cpu": resource.MustParse("6"), "memory": resource.MustParse("6Gi")},
			AllocatedResources: map[int32]api.ComputeResource{
				0: {
					Resources: map[string]resource.Quantity{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3.5Gi")},
				},
			},
		},
	}

	aggregated := AggregateNodeTypeAllocations(nodes)
	expected := []*nodeTypeAllocation{
		{
			nodeType: api.NodeType{
				Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}, {Key: "two", Value: "2", Effect: "NoSchedule"}},
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("5"), "memory": resource.MustParse("5Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
			totalResources:     common.ComputeResourcesFloat{"cpu": 6, "memory": 6 * 1024 * 1024 * 1024},
			allocatedResources: map[int32]common.ComputeResourcesFloat{
				0: {"cpu": 1, "memory": 3.5 * 1024 * 1024 * 1024},
			},
		},
		{
			nodeType: api.NodeType{
				Taints:               []v1.Taint{{Key: "one", Value: "1", Effect: "NoSchedule"}},
				Labels:               nil,
				AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("1"), "memory": resource.MustParse("3Gi")},
			},
			availableResources: common.ComputeResourcesFloat{"cpu": 2, "memory": 1 * 1024 * 1024 * 1024},
			totalResources:     common.ComputeResourcesFloat{"cpu": 2, "memory": 1 * 1024 * 1024 * 1024},
			allocatedResources: map[int32]common.ComputeResourcesFloat{
				0: {"cpu": 1, "memory": 2.5 * 1024 * 1024 * 1024},
			},
		},
	}
	for i := range aggregated {
		assert.Equal(t, expected[i], aggregated[i])
	}
}
