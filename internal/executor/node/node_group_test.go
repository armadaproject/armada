package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
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
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))

	node := createNodeWithTaints("node1", "tolerated1")
	node.Labels = map[string]string{nodeTypeLabel: "example-node-type"}

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "example-node-type")
}

func TestGetType_WhenNodeHasUntoleratedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))
	node := createNodeWithTaints("node1", "untolerated")

	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, defaultNodeType)
}

func TestGetType_WhenNodeHasToleratedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))

	node := createNodeWithTaints("node1", "tolerated1")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1")

	node = createNodeWithTaints("node1", "tolerated1", "tolerated2")
	result = nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1,tolerated2")
}

func TestGetType_WhenSomeNodeTaintsTolerated(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))

	node := createNodeWithTaints("node1", "tolerated1", "untolerated")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1")
}

func TestGetType_WhenNodeHasRecognizedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{Keys: []string{"recognized1", "recognized2"}}))

	node := createNodeWithTaints("node1", "recognized1", "recognized2")
	assert.Equal(t, "recognized1,recognized2", nodeInfoService.GetType(node))
}

func TestGroupNodesByType(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"tolerated1", "tolerated2"}, configuration.RecognizedTaintsConfiguration{}))

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

func TestGroupNodesByType_WhenNodesHaveRecognizedTaints(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{Keys: []string{"recognized1", "recognized2"}}))

	node1 := createNodeWithTaints("node1", "recognized1")
	node2 := createNodeWithTaints("node2", "recognized1", "recognized2")
	groups := nodeInfoService.GroupNodesByType([]*v1.Node{node1, node2})

	assert.ElementsMatch(t, []*NodeGroup{
		{NodeType: "recognized1", Nodes: []*v1.Node{node1}},
		{NodeType: "recognized1,recognized2", Nodes: []*v1.Node{node2}},
	}, groups)
}

func TestFilterAvailableProcessingNodes(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{}))

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
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{}))

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
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{}))

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
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"taint"}, configuration.RecognizedTaintsConfiguration{}))

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

func TestFilterAvailableProcessingNodes_IsTrue_NodeWithRecognizedTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{Keys: []string{"taint"}}))

	node := v1.Node{Spec: v1.NodeSpec{Taints: []v1.Taint{{Key: "taint", Effect: v1.TaintEffectNoSchedule}}}}
	assert.True(t, nodeInfoService.IsAvailableProcessingNode(&node))
}

func TestFilterAvailableProcessingNodes_LegacyToleratedTaintsAreLiteral(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{"example.com/dedicated"}, configuration.RecognizedTaintsConfiguration{}))

	for _, tc := range []struct {
		key       string
		available bool
	}{
		{key: "example.com/dedicated", available: true},
		{key: "exampleXcom/dedicated"},
		{key: "prefix-example.com/dedicated"},
		{key: "example.com/dedicated-extra"},
		{key: constants.ReservationTaintKey, available: true},
		{key: "prefix-" + constants.ReservationTaintKey},
		{key: constants.ReservationTaintKey + "-extra"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			node := v1.Node{Spec: v1.NodeSpec{Taints: []v1.Taint{{Key: tc.key, Effect: v1.TaintEffectNoSchedule}}}}
			assert.Equal(t, tc.available, nodeInfoService.IsAvailableProcessingNode(&node))
		})
	}
}

func TestGetType_WhenNodeMatchesAdminRecognizedTaintPattern(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustRecognizedTaints(t, []string{}, configuration.RecognizedTaintsConfiguration{
			Admin: []configuration.RecognizedTaintAdminConfiguration{
				{
					Category: "platform",
					Keys:     []string{`example\.com/.*`},
				},
			},
		}))

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

func mustRecognizedTaints(t *testing.T, toleratedTaints []string, recognizedTaints configuration.RecognizedTaintsConfiguration) *configuration.RecognizedTaints {
	t.Helper()
	result, err := (configuration.KubernetesConfiguration{
		ToleratedTaints:  toleratedTaints,
		RecognizedTaints: recognizedTaints,
	}).NewRecognizedTaints()
	require.NoError(t, err)
	return result
}
