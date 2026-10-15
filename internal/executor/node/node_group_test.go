package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/armadaproject/armada/internal/common/constants"
	"github.com/armadaproject/armada/internal/executor/configuration"
	fakeContext "github.com/armadaproject/armada/internal/executor/fake/context"
)

var (
	testAppConfig          = configuration.ApplicationConfiguration{ClusterId: "test", Pool: "pool"}
	nodeTypeLabel          = "node-type"
	nodePoolLabel          = "node-pool"
	reservedNodePoolSuffix = "-reserved"
)

func TestGetPool_WhenNodeHasNodePoolLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{},
	)
	node := createNodeWithTaints("node1")
	node.Labels = map[string]string{nodePoolLabel: "example-node-pool"}

	result := nodeInfoService.GetPool(node)
	assert.Equal(t, result, "example-node-pool")
}

func TestGetPool_WhenNodeDoesNotHaveNodePoolLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{},
	)
	node := createNodeWithTaints("node1")

	result := nodeInfoService.GetPool(node)
	// Should default to cluster configured pool
	assert.Equal(t, result, testAppConfig.Pool)
}

func TestGetPool_WhenReservedNodeHasNodePoolLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{},
	)
	node := createNodeWithTaints("node1", constants.ReservationTaintKey)
	node.Labels = map[string]string{nodePoolLabel: "example-node-pool"}

	result := nodeInfoService.GetPool(node)
	assert.Equal(t, result, "example-node-pool-reserved")
}

func TestGetPool_WhenReservedNodeHasNodePoolLabel_SuffixDisabled(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		"",
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{},
	)
	node := createNodeWithTaints("node1", constants.ReservationTaintKey)
	node.Labels = map[string]string{nodePoolLabel: "example-node-pool"}

	result := nodeInfoService.GetPool(node)
	assert.Equal(t, result, "example-node-pool")
}

func TestGetType_WhenNodeHasNoTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{},
	)
	node := createNodeWithTaints("node1")

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, defaultNodeType)
}

func TestGetType_WhenNodeHasNodeTypeLabel(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"queueable1"}},
	)

	node := createNodeWithTaints("node1", "queueable1")
	node.Labels = map[string]string{nodeTypeLabel: "example-node-type"}

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "example-node-type")
}

func TestGetType_WhenNodeHasNonQueueableTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"queueable1", "queueable2"}},
	)
	node := createNodeWithTaints("node1", "non-queueable")

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, defaultNodeType)
}

func TestGetType_WhenNodeHasQueueableTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"queueable1", "queueable2"}},
	)

	node := createNodeWithTaints("node1", "queueable1")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "queueable1")

	node = createNodeWithTaints("node1", "queueable1", "queueable2")
	result = nodeInfoService.GetType(node)
	assert.Equal(t, result, "queueable1,queueable2")
}

func TestGetType_WhenSomeNodeTaintsAreQueueable(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"queueable1", "queueable2"}},
	)

	node := createNodeWithTaints("node1", "queueable1", "non-queueable")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "queueable1")
}

func TestGroupNodesByType(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"tolerated1", "tolerated2"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"queueable1", "queueable2"}},
	)

	node1 := createNodeWithTaints("node1")
	node2 := createNodeWithTaints("node2", "non-queueable")
	node3 := createNodeWithTaints("node3", "queueable1")
	node4 := createNodeWithTaints("node4", "queueable1", "non-queueable")
	node5 := createNodeWithTaints("node5", "queueable1", "queueable2")

	groupedNodes := nodeInfoService.GroupNodesByType([]*v1.Node{node1, node2, node3, node4, node5})
	assert.Equal(t, len(groupedNodes), 3)

	expected := map[string][]*v1.Node{
		defaultNodeType:         {node1, node2},
		"queueable1":            {node3, node4},
		"queueable1,queueable2": {node5},
	}

	for _, nodeGroup := range groupedNodes {
		expectedGroup, present := expected[nodeGroup.NodeType]
		assert.True(t, present)
		assert.Equal(t, expectedGroup, nodeGroup.Nodes)
	}
}

func TestFilterAvailableProcessingNodes(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{},
		configuration.QueueableTaintsConfiguration{},
	)

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
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{},
		configuration.QueueableTaintsConfiguration{},
	)

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
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{},
		configuration.QueueableTaintsConfiguration{},
	)

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

func TestFilterAvailableProcessingNodes_IsTrue_NodeWithQueueableTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{"taint"},
		configuration.QueueableTaintsConfiguration{Keys: []string{"taint"}},
	)

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

func TestGetType_WhenNodeMatchesAdminQueueableTaintPattern(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		[]string{},
		configuration.QueueableTaintsConfiguration{
			Admin: []configuration.QueueableTaintAdminConfiguration{
				{
					Category: "platform",
					Keys:     []string{`example\.com/.*`},
				},
			},
		})

	node := createNodeWithTaints("node1", "example.com/dedicated")
	assert.Equal(t, "example.com/dedicated", nodeInfoService.GetType(node))
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
