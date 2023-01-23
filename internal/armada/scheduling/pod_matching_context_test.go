package scheduling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
)

func Test_Matches_BasicPod_ReturnsTrue(t *testing.T) {
	podSpec := &v1.PodSpec{}
	ctx := NewPodMatchingContext(podSpec)

	available := makeResourceList(1, 10).AsFloat()
	nodeType := &api.NodeType{}

	ok, err := ctx.Matches(nodeType, available)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_fits(t *testing.T) {
	available := makeResourceList(1, 10).AsFloat()

	ok, err := fits(makeResourceList(2, 20).AsFloat(), available)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = fits(makeResourceList(2, 5).AsFloat(), available)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = fits(makeResourceList(1, 10).AsFloat(), available)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_matchNodeSelector(t *testing.T) {
	labels := map[string]string{
		"A": "test",
		"B": "test",
	}

	ok, err := matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"C": "test"}}, labels)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"B": "42"}}, labels)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()

	ok, err = matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"A": "test"}}, labels)
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = matchNodeSelector(&v1.PodSpec{NodeSelector: map[string]string{"A": "test", "B": "test"}}, labels)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_tolerates_WhenTaintHasNoToleration_ReturnsFalse(t *testing.T) {
	taints := makeTaints()
	podSpec := &v1.PodSpec{}
	ok, err := tolerates(podSpec, taints)
	assert.False(t, ok)
	assert.Error(t, err)
	err.Error()
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
		},
	}

	ok, err := tolerates(podSpec, taints)
	assert.True(t, ok)
	assert.NoError(t, err)
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

	ok, err := tolerates(podSpec, taints)
	assert.True(t, ok)
	assert.NoError(t, err)
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

	ok, err := tolerates(podSpec, taints)
	assert.False(t, ok)
	assert.Error(t, err)
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

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[0], resultNode)
	assert.True(t, resultFlag)
	assert.NoError(t, err)
}

func Test_matchAnyNodeTypePodAllocation_WhenAllAvailableCpuConsumed_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{"cpu": *resource.NewQuantity(1, ""), "memory": *resource.NewQuantity(1, "")},
				},
			},
		},
	}
	nodeAllocations := defaultNodeTypeAllocations()
	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 4, "memory": 1 * 1024 * 1024 * 1024}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{"cpu": 4, "memory": 1 * 1024 * 1024 * 1024}}

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
	assert.Error(t, err)
}

func Test_matchAnyNodeTypePodAllocation_WhenNodeSelectorMatchesNoNodes_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{NodeSelector: map[string]string{"a": "b"}}
	nodeAllocations := defaultNodeTypeAllocations()
	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
	assert.Error(t, err)
}

func Test_matchAnyNodeTypePodAllocation_WhenNodeSelectorRulesOutFirstNode_ReturnsSecondNode(t *testing.T) {
	podSpec := &v1.PodSpec{NodeSelector: map[string]string{"a": "b"}}

	nodeAllocations := []*nodeTypeAllocation{defaultNodeTypeAllocation(), defaultNodeTypeAllocation()}
	nodeAllocations[0].nodeType.Labels = map[string]string{"a": "does-not-match"}
	nodeAllocations[1].nodeType.Labels = map[string]string{"a": "b"}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[1], resultNode)
	assert.True(t, resultFlag)
	assert.NoError(t, err)
}

func Test_matchAnyNodeTypePodAllocation_WhenTaintRulesOutAllNodes_ReturnsFalse(t *testing.T) {
	podSpec := &v1.PodSpec{}

	nodeAllocations := defaultNodeTypeAllocations()
	nodeAllocations[0].nodeType.Taints = []v1.Taint{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Nil(t, resultNode)
	assert.False(t, resultFlag)
	assert.Error(t, err)
}

func Test_matchAnyNodeTypePodAllocation_WhenTaintRulesOutFirstNode_ReturnsSecondNode(t *testing.T) {
	podSpec := &v1.PodSpec{Tolerations: []v1.Toleration{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}}

	nodeAllocations := []*nodeTypeAllocation{defaultNodeTypeAllocation(), defaultNodeTypeAllocation()}
	nodeAllocations[0].nodeType.Taints = []v1.Taint{{Key: "c", Value: "does-not-match", Effect: v1.TaintEffectNoSchedule}}
	nodeAllocations[1].nodeType.Taints = []v1.Taint{{Key: "a", Value: "b", Effect: v1.TaintEffectNoSchedule}}

	alreadyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}
	newlyConsumed := nodeTypeUsedResources{nodeAllocations[0]: common.ComputeResourcesFloat{}}

	resultNode, resultFlag, err := matchAnyNodeTypePodAllocation(podSpec, nodeAllocations, alreadyConsumed, newlyConsumed)
	assert.Equal(t, nodeAllocations[1], resultNode)
	assert.True(t, resultFlag)
	assert.NoError(t, err)
}

func Test_matchesRequiredNodeAffinity_WhenNoAffinitySet_ReturnsTrue(t *testing.T) {
	podSpec := &v1.PodSpec{}
	nodeType := &api.NodeType{}

	ok, err := matchesRequiredNodeAffinity(makeRequiredNodeAffinitySelector(podSpec), nodeType)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_matchesRequiredNodeAffinity_WhenInAffinitySet_MatchesOnlyLabelledNode(t *testing.T) {
	reqs := []v1.NodeSelectorRequirement{{
		Key:      "a",
		Operator: v1.NodeSelectorOpIn,
		Values:   []string{"b"},
	}}

	podSpec := podWithRequiredNodeAffinity(reqs)
	sel := makeRequiredNodeAffinitySelector(podSpec)

	ok, err := matchesRequiredNodeAffinity(sel, &api.NodeType{})
	assert.False(t, ok)
	assert.Error(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{}})
	assert.False(t, ok)
	assert.Error(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"does_not_match": "does_not_match"}})
	assert.False(t, ok)
	assert.Error(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"a": "does_not_match"}})
	assert.False(t, ok)
	assert.Error(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"a": "b"}})
	assert.True(t, ok)
	assert.NoError(t, err)
}

func Test_matchesRequiredNodeAffinity_WhenNotInAffinitySet_MatchesAllExceptLabelledNode(t *testing.T) {
	reqs := []v1.NodeSelectorRequirement{{
		Key:      "a",
		Operator: v1.NodeSelectorOpNotIn,
		Values:   []string{"b"},
	}}

	podSpec := podWithRequiredNodeAffinity(reqs)
	sel := makeRequiredNodeAffinitySelector(podSpec)

	ok, err := matchesRequiredNodeAffinity(sel, &api.NodeType{})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{}})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"does_not_match": "does_not_match"}})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"a": "does_not_match"}})
	assert.True(t, ok)
	assert.NoError(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"a": "b"}})
	assert.False(t, ok)
	assert.Error(t, err)

	ok, err = matchesRequiredNodeAffinity(sel, &api.NodeType{Labels: map[string]string{"a": "b", "does_not_match": "does_not_match"}})
	assert.False(t, ok)
	assert.Error(t, err)
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

func podWithRequiredNodeAffinity(reqs []v1.NodeSelectorRequirement) *v1.PodSpec {
	requiredTerms := []v1.NodeSelectorTerm{
		{MatchExpressions: reqs},
	}
	return &v1.PodSpec{
		Affinity: &v1.Affinity{
			NodeAffinity: &v1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &v1.NodeSelector{NodeSelectorTerms: requiredTerms}},
		},
	}
}
