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

func Test_tolerates_WhenTaintHasNoToleration_ReturnsFalse(t *testing.T) {
	taints := makeTaints()
	podSpec := &v1.PodSpec{}
	assert.False(t, tolerates(podSpec, taints))
}

func Test_tolerates_WhenTaintHasAnEqualsToleration_ReturnsTrue(t *testing.T) {
	taints := makeTaints()

	podSpec := &v1.PodSpec{
		Tolerations: []v1.Toleration{
			{
				Key:      "A",
				Operator: v1.TolerationOpEqual,
				Value:    "test",
				Effect:   v1.TaintEffectNoSchedule,
			},
		}}

	assert.True(t, tolerates(podSpec, taints))
}

func Test_tolerates_WhenTaintHasAnExistsToleration_ReturnsTrue(t *testing.T) {
	taints := makeTaints()

	podSpec := &v1.PodSpec{
		Tolerations: []v1.Toleration{
			{
				Key:      "A",
				Operator: v1.TolerationOpExists,
				Effect:   v1.TaintEffectNoSchedule,
			},
		},
	}

	assert.True(t, tolerates(podSpec, taints))
}

func Test_tolerates_WhenTaintHasAToleration_ButEffectDiffers_ReturnsFalse(t *testing.T) {
	taints := makeTaints()

	podSpec := &v1.PodSpec{
		Tolerations: []v1.Toleration{
			{
				Key:      "A",
				Operator: v1.TolerationOpExists,
				Effect:   v1.TaintEffectNoExecute,
			},
		},
	}

	assert.False(t, tolerates(podSpec, taints))
}

func makeTaints() []v1.Taint {
	return []v1.Taint{
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
}

func Test_matchAnyNodeTypePodAllocation_WhenFindsMatch_ReturnsMatch(t *testing.T) {
	podSpec := &v1.PodSpec{}
	nodeAllocations := defaultNodeTypeAllocations()
	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 3, "memory": 1 * 1024 * 1024 * 1024}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 3, "memory": 1 * 1024 * 1024 * 1024}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[0], resultNode)
	assert.True(t, resultFlag)
}

func Test_matchAnyNodeTypePodAllocation_WhenAllAvailableCpuConsumed_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{}
	nodeAllocations := defaultNodeTypeAllocations()
	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 4, "memory": 1 * 1024 * 1024 * 1024}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 4, "memory": 1 * 1024 * 1024 * 1024}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
}

func Test_matchAnyNodeTypePodAllocation_WhenNodeSelectorMatchesNoNodes_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{NodeSelector: map[string]string{"a": "b"}}
	nodeAllocations := defaultNodeTypeAllocations()
	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
}
func Test_matchAnyNodeTypePodAllocation_WhenNodeSelectorRulesOutFirstNode_ReturnsSecondNode(t *testing.T) {
	podSpec := &v1.PodSpec{NodeSelector: map[string]string{"a": "b"}}

	nodeAllocations := []*nodeTypeAllocation{defaultNodeTypeAllocation(), defaultNodeTypeAllocation()}
	nodeAllocations[0].nodeType.Labels = map[string]string{"a": "does-not-match"}
	nodeAllocations[1].nodeType.Labels = map[string]string{"a": "b"}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[1], resultNode)
	assert.True(t, resultFlag)
}

func Test_matchAnyNodeTypePodAllocation_WhenTaintRulesOutAllNodes_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{}

	nodeAllocations := defaultNodeTypeAllocations()
	nodeAllocations[0].nodeType.Taints = []v1.Taint{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
}
func Test_matchAnyNodeTypePodAllocation_WhenTaintRulesOutFirstNode_ReturnsSecondNode(t *testing.T) {
	podSpec := &v1.PodSpec{Tolerations: []v1.Toleration{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}}

	nodeAllocations := []*nodeTypeAllocation{defaultNodeTypeAllocation(), defaultNodeTypeAllocation()}
	nodeAllocations[0].nodeType.Taints = []v1.Taint{{Key: "c", Value: "does-not-match", Effect: v1.TaintEffectNoSchedule}}
	nodeAllocations[1].nodeType.Taints = []v1.Taint{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[1], resultNode)
	assert.True(t, resultFlag)
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

func defaultNodeTypeAllocations() []*nodeTypeAllocation {
	return []*nodeTypeAllocation{defaultNodeTypeAllocation()}
}

func defaultNodeTypeAllocation() *nodeTypeAllocation {
	return &nodeTypeAllocation{
		nodeType: api.NodeType{
			Taints:               nil,
			Labels:               nil,
			AllocatableResources: common.ComputeResources{"cpu": resource.MustParse("8"), "memory": resource.MustParse("8Gi")},
		},
		availableResources: common.ComputeResourcesFloat{"cpu": 7, "memory": 7 * 1024 * 1024 * 1024},
	}
}
