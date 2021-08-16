package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetType_WhenNodeHasNoTaint(t *testing.T) {
	nodeGroupService := NewKubernetesNodeInfoService("cpu", []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1")

	result := nodeGroupService.GetType(node)
	assert.Equal(t, result.Id, nodeGroupService.clusterPool)
	assert.Equal(t, len(result.Taints), 0)
}

func TestGetType_WhenNodeHasUntoleratedTaint(t *testing.T) {
	nodeGroupService := NewKubernetesNodeInfoService("cpu", []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1", "untolerated")

	result := nodeGroupService.GetType(node)
	assert.Equal(t, result.Id, nodeGroupService.clusterPool)
	assert.Equal(t, len(result.Taints), 0)
}

func TestGetType_WhenNodeHasToleratedTaint(t *testing.T) {
	nodeGroupService := NewKubernetesNodeInfoService("cpu", []string{"tolerated1", "tolerated2"})

	node := createNodeWithTaints("node1", "tolerated1")
	result := nodeGroupService.GetType(node)
	assert.Equal(t, result.Id, "tolerated1")
	assert.Equal(t, len(result.Taints), 1)
	assert.Equal(t, result.Taints, node.Spec.Taints)

	node = createNodeWithTaints("node1", "tolerated1", "tolerated2")
	result = nodeGroupService.GetType(node)
	assert.Equal(t, result.Id, "tolerated1,tolerated2")
	assert.Equal(t, len(result.Taints), 2)
	assert.Equal(t, result.Taints, node.Spec.Taints)
}

func TestGetType_WhenSomeNodeTaintsTolerated(t *testing.T) {
	nodeGroupService := NewKubernetesNodeInfoService("cpu", []string{"tolerated1", "tolerated2"})

	node := createNodeWithTaints("node1", "tolerated1", "untolerated")
	result := nodeGroupService.GetType(node)
	assert.Equal(t, result.Id, "tolerated1")
	assert.Equal(t, len(result.Taints), 1)
	assert.Equal(t, result.Taints[0], node.Spec.Taints[0])
}

func TestGroupNodesByType(t *testing.T) {
	nodeGroupService := NewKubernetesNodeInfoService("cpu", []string{"tolerated1", "tolerated2"})

	node1 := createNodeWithTaints("node1")
	node2 := createNodeWithTaints("node2", "untolerated")
	node3 := createNodeWithTaints("node3", "tolerated1")
	node4 := createNodeWithTaints("node4", "tolerated1", "untolerated")
	node5 := createNodeWithTaints("node5", "tolerated1", "tolerated2")

	groupedNodes := nodeGroupService.GroupNodesByType([]*v1.Node{node1, node2, node3, node4, node5})
	assert.Equal(t, len(groupedNodes), 3)

	expected := map[string][]*v1.Node{
		nodeGroupService.clusterPool: {node1, node2},
		"tolerated1":                 {node3, node4},
		"tolerated1,tolerated2":      {node5},
	}

	for _, nodeGroup := range groupedNodes {
		expectedGroup, present := expected[nodeGroup.NodeType.Id]
		assert.True(t, present)
		assert.Equal(t, expectedGroup, nodeGroup.Nodes)
	}
}

func createNodeWithTaints(nodeName string, taintNames ...string) *v1.Node {
	taints := []v1.Taint{}
	for _, taintName := range taintNames {
		taints = append(taints, v1.Taint{
			Key:    taintName,
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		})
	}
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Spec: v1.NodeSpec{
			Taints: taints,
		},
	}
}
