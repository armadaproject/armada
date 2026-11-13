package node

import (
	"fmt"
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)

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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)
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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)

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
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)

	node := createNodeWithTaints("node1", "tolerated1", "untolerated")
	result := nodeInfoService.GetType(node)
	assert.Equal(t, result, "tolerated1")
}

func TestGetType_WhenNodeHasWellKnownTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{Keys: []string{"recognized1", "recognized2"}}), false)

	node := createNodeWithTaints("node1", "recognized1", "recognized2")
	assert.Equal(t, "recognized1,recognized2", nodeInfoService.GetType(node))
}

func TestGroupNodesByType(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{"tolerated1", "tolerated2"}, configuration.WellKnownTaintsConfiguration{}), false)

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

func TestGroupNodesByType_WhenNodesHaveWellKnownTaints(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{Keys: []string{"recognized1", "recognized2"}}), false)

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
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{}), false)

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
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{}), false)

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
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{}), false)

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
		mustWellKnownTaints(t, []string{"taint"}, configuration.WellKnownTaintsConfiguration{}), false)

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

func TestFilterAvailableProcessingNodes_IsTrue_NodeWithWellKnownTaint(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{Keys: []string{"taint"}}), false)

	node := v1.Node{Spec: v1.NodeSpec{Taints: []v1.Taint{{Key: "taint", Effect: v1.TaintEffectNoSchedule}}}}
	assert.True(t, nodeInfoService.IsAvailableProcessingNode(&node))
}

func TestIsAvailableProcessingNode_CategoriseUnknownTaints(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	for name, tc := range map[string]struct {
		categoriseUnknown bool
		unschedulable     bool
		taints            []v1.Taint
		want              bool
	}{
		"unknown NoSchedule rejected when disabled": {taints: []v1.Taint{{Key: "unknown", Effect: v1.TaintEffectNoSchedule}}},
		"unknown NoSchedule accepted when enabled":  {categoriseUnknown: true, taints: []v1.Taint{{Key: "unknown", Effect: v1.TaintEffectNoSchedule}}, want: true},
		"known NoSchedule accepted when disabled":   {taints: []v1.Taint{{Key: "known", Effect: v1.TaintEffectNoSchedule}}, want: true},
		"unknown NoExecute accepted when disabled":  {taints: []v1.Taint{{Key: "unknown", Effect: v1.TaintEffectNoExecute}}, want: true},
		"cordoned rejected when enabled":            {categoriseUnknown: true, unschedulable: true, taints: []v1.Taint{{Key: "unknown", Effect: v1.TaintEffectNoSchedule}}},
	} {
		t.Run(name, func(t *testing.T) {
			config := configuration.TaintCategorisationConfiguration{
				CategoriseUnknownTaints: tc.categoriseUnknown,
				WellKnownTaints:         configuration.WellKnownTaintsConfiguration{Keys: []string{"known"}},
			}
			wellKnownTaints, err := (configuration.KubernetesConfiguration{TaintCategorisation: config}).NewWellKnownTaints()
			require.NoError(t, err)
			service := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, reservedNodePoolSuffix, wellKnownTaints, tc.categoriseUnknown)
			node := &v1.Node{Spec: v1.NodeSpec{Unschedulable: tc.unschedulable, Taints: tc.taints}}
			assert.Equal(t, tc.want, service.IsAvailableProcessingNode(node))
		})
	}
}

func TestIsAvailableProcessingNode_ControlPlaneRoles(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	for _, categoriseUnknown := range []bool{false, true} {
		config := configuration.TaintCategorisationConfiguration{
			CategoriseUnknownTaints: categoriseUnknown,
			WellKnownTaints: configuration.WellKnownTaintsConfiguration{Keys: []string{
				"node-role.kubernetes.io/master", "node-role.kubernetes.io/control-plane",
			}},
		}
		wellKnownTaints, err := (configuration.KubernetesConfiguration{TaintCategorisation: config}).NewWellKnownTaints()
		require.NoError(t, err)
		service := NewKubernetesNodeInfoService(context, nodeTypeLabel, nodePoolLabel, reservedNodePoolSuffix, wellKnownTaints, categoriseUnknown)
		for name, tc := range map[string]struct {
			labels map[string]string
			taints []v1.Taint
			want   bool
		}{
			"master label":        {labels: map[string]string{"node-role.kubernetes.io/master": ""}},
			"control-plane label": {labels: map[string]string{"node-role.kubernetes.io/control-plane": "true"}},
			"master taint":        {taints: []v1.Taint{{Key: "node-role.kubernetes.io/master", Effect: v1.TaintEffectNoSchedule}}},
			"control-plane taint": {taints: []v1.Taint{{Key: "node-role.kubernetes.io/control-plane", Value: "true", Effect: v1.TaintEffectNoExecute}}},
			"worker":              {labels: map[string]string{"node-role.kubernetes.io/worker": ""}, want: true},
		} {
			t.Run(fmt.Sprintf("categorise=%t/%s", categoriseUnknown, name), func(t *testing.T) {
				node := &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: tc.labels}, Spec: v1.NodeSpec{Taints: tc.taints}}
				assert.Equal(t, tc.want, service.IsAvailableProcessingNode(node))
			})
		}
	}
}

func TestNewWellKnownTaints_RejectsInvalidTaintPattern(t *testing.T) {
	config := configuration.KubernetesConfiguration{
		TaintCategorisation: configuration.TaintCategorisationConfiguration{
			WellKnownTaints: configuration.WellKnownTaintsConfiguration{Keys: []string{"["}},
		},
	}
	_, err := config.NewWellKnownTaints()
	require.Error(t, err)
}

func TestFilterAvailableProcessingNodes_LegacyToleratedTaintsAreLiteral(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{"example.com/dedicated"}, configuration.WellKnownTaintsConfiguration{}), false)

	for key, tc := range map[string]struct {
		available bool
	}{
		"example.com/dedicated":                   {available: true},
		"exampleXcom/dedicated":                   {},
		"prefix-example.com/dedicated":            {},
		"example.com/dedicated-extra":             {},
		constants.ReservationTaintKey:             {available: true},
		"prefix-" + constants.ReservationTaintKey: {},
		constants.ReservationTaintKey + "-extra":  {},
	} {
		t.Run(key, func(t *testing.T) {
			node := v1.Node{Spec: v1.NodeSpec{Taints: []v1.Taint{{Key: key, Effect: v1.TaintEffectNoSchedule}}}}
			assert.Equal(t, tc.available, nodeInfoService.IsAvailableProcessingNode(&node))
		})
	}
}

func TestGetType_WhenNodeMatchesAdminWellKnownTaintPattern(t *testing.T) {
	context := fakeContext.NewFakeClusterContext(testAppConfig, "kubernetes.io/hostname", nil)
	nodeInfoService := NewKubernetesNodeInfoService(context,
		nodeTypeLabel,
		nodePoolLabel,
		reservedNodePoolSuffix,
		mustWellKnownTaints(t, []string{}, configuration.WellKnownTaintsConfiguration{
			Admin: []configuration.WellKnownTaintAdminConfiguration{
				{
					Category: "platform",
					Keys:     []string{`example\.com/.*`},
				},
			},
		}), false)

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

func mustWellKnownTaints(t *testing.T, toleratedTaints []string, wellKnownTaints configuration.WellKnownTaintsConfiguration) *configuration.WellKnownTaints {
	t.Helper()
	config := configuration.KubernetesConfiguration{
		ToleratedTaints:     toleratedTaints,
		TaintCategorisation: configuration.TaintCategorisationConfiguration{WellKnownTaints: wellKnownTaints},
	}
	matcher, err := config.NewWellKnownTaints()
	require.NoError(t, err)
	return matcher
}
