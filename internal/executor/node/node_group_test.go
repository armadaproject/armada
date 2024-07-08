package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/executor/configuration"
	fakeContext "github.com/armadaproject/armada/internal/executor/fake/context"
)

var (
	testAppConfig = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}
	nodeTypeLabel = "node-type"
	nodePoolLabel = "node-pool"
)

func TestGetPool_WhenNodeHasNodePoolLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1")
	node.Labels = map[string]string{nodePoolLabel: "example-node-pool"}

	result := nodeInfoService.GetPool(node)
	assert.Equal(t, result, "example-node-pool")
}

func TestGetPool_WhenNodeDoesNotHaveNodePoolLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1")

	result := nodeInfoService.GetPool(node)
	// Should default to cluster configured pool
	assert.Equal(t, result, testAppConfig.Pool)
}

func TestGetType_WhenNodeHasNoTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1")

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, defaultNodeType)
}

func TestGetType_WhenNodeHasNodeTypeLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})

	node := createNodeWithTaints("node1", "tolerated1")
	node.Labels = map[string]string{nodeTypeLabel: "example-node-type"}

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "example-node-type")
}

func TestGetType_WhenNodeHasUntoleratedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})
	node := createNodeWithTaints("node1", "untolerated")

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, defaultNodeType)
}

func TestGetType_WhenNodeHasToleratedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})

	node := createNodeWithTaints("node1", "tolerated1")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1")

	node = createNodeWithTaints("node1", "tolerated1", "tolerated2")
	result = nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1,tolerated2")
}

func TestGetType_WhenSomeNodeTaintsTolerated(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})

	node := createNodeWithTaints("node1", "tolerated1", "untolerated")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1")
}

func TestGroupNodesByType(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"tolerated1", "tolerated2"})

	node1 := createNodeWithTaints("node1")
	node2 := createNodeWithTaints("node2", "untolerated")
	node3 := createNodeWithTaints("node3", "tolerated1")
	node4 := createNodeWithTaints("node4", "tolerated1", "untolerated")
	node5 := createNodeWithTaints("node5", "tolerated1", "tolerated2")

	groupedNodes := nodeInfoService.GroupNodesByType([]*v1.Node{node1, node2, node3, node4, node5})
	assert.Equal(t, len(groupedNodes), 3)

	expected := map[string][]*v1.Node{
		defaultNodeType:         {node1, node2},
		"tolerated1":            {node3, node4},
		"tolerated1,tolerated2": {node5},
	}

	for _, nodeGroup := range groupedNodes {
		expectedGroup, present := expected[nodeGroup.NodeType]
		assert.True(t, present)
		assert.Equal(t, expectedGroup, nodeGroup.Nodes)
	}
}

func TestFilterAvailableProcessingNodes(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{})

	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        nil,
		},
	}

	result := nodeInfoService.IsAvailableProcessingNode(&node)
	assert.True(t, result, 1)
}

func TestIsAvailableProcessingNode_IsFalse_UnschedulableNode(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{})

	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: true,
			Taints:        nil,
		},
	}

	result := nodeInfoService.IsAvailableProcessingNode(&node)
	assert.False(t, result)
}

func TestFilterAvailableProcessingNodes_IsFalse_NodeWithNoScheduleTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{})

	taint := v1.Taint{
		Key:    "taint",
		Effect: v1.TaintEffectNoSchedule,
	}
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        []v1.Taint{taint},
		},
	}

	result := nodeInfoService.IsAvailableProcessingNode(&node)
	assert.False(t, result)
}

func TestFilterAvailableProcessingNodes_IsTrue_NodeWithToleratedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, []string{"taint"})

	taint := v1.Taint{
		Key:    "taint",
		Effect: v1.TaintEffectNoSchedule,
	}
	node := v1.Node{
		Spec: v1.NodeSpec{
			Unschedulable: false,
			Taints:        []v1.Taint{taint},
		},
	}

	result := nodeInfoService.IsAvailableProcessingNode(&node)
	assert.True(t, result)
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
