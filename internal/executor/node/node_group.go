package node

import (
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/executor/context"
	util2 "github.com/armadaproject/armada/internal/executor/util"
)

const defaultNodeType = "none"

type NodeInfoService interface {
	IsAvailableProcessingNode(*v1.Node) bool
	GetAllAvailableProcessingNodes() ([]*v1.Node, error)
	GetAllNodes() ([]*v1.Node, error)
	GroupNodesByType(nodes []*v1.Node) []*NodeGroup
	GetType(node *v1.Node) string
	GetPool(node *v1.Node) string
}

type KubernetesNodeInfoService struct {
	clusterContext  context.ClusterContext
	nodeTypeLabel   string
	nodePoolLabel   string
	toleratedTaints map[string]bool
}

func NewKubernetesNodeInfoService(
	clusterContext context.ClusterContext,
	nodeTypeLabel string,
	nodePoolLabel string,
	toleratedTaints []string,
) *KubernetesNodeInfoService {
	return &KubernetesNodeInfoService{
		clusterContext:  clusterContext,
		nodePoolLabel:   nodePoolLabel,
		nodeTypeLabel:   nodeTypeLabel,
		toleratedTaints: util.StringListToSet(toleratedTaints),
	}
}

type NodeGroup struct {
	NodeType string
	Nodes    []*v1.Node
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GroupNodesByType(nodes []*v1.Node) []*NodeGroup {
	nodeGroupMap := map[string]*NodeGroup{}

	for _, node := range nodes {
		nodeType := kubernetesNodeInfoService.GetType(node)
		if _, present := nodeGroupMap[nodeType]; !present {
			nodeGroupMap[nodeType] = &NodeGroup{
				NodeType: nodeType,
				Nodes:    []*v1.Node{},
			}
		}
		nodeGroupMap[nodeType].Nodes = append(nodeGroupMap[nodeType].Nodes, node)
	}

	nodeGroups := make([]*NodeGroup, 0, len(nodeGroupMap))
	for _, group := range nodeGroupMap {
		nodeGroups = append(nodeGroups, group)
	}

	return nodeGroups
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GetPool(node *v1.Node) string {
	// For now default to the cluster pool
	// Once node pool rolled out for a while it'd be ideal to remove the concept of "cluster" pool
	//  and just default to a static string
	nodePool := kubernetesNodeInfoService.clusterContext.GetClusterPool()

	if labelValue, ok := node.Labels[kubernetesNodeInfoService.nodePoolLabel]; ok {
		nodePool = labelValue
	}

	return nodePool
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GetType(node *v1.Node) string {
	nodeType := defaultNodeType

	if labelValue, ok := node.Labels[kubernetesNodeInfoService.nodeTypeLabel]; ok {
		nodeType = labelValue
	} else {
		relevantTaints := kubernetesNodeInfoService.filterToleratedTaints(node.Spec.Taints)
		if len(relevantTaints) > 0 {
			nodeType = nodeGroupId(relevantTaints)
		}
	}

	return nodeType
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) filterToleratedTaints(taints []v1.Taint) []v1.Taint {
	result := []v1.Taint{}

	for _, taint := range taints {
		_, ok := kubernetesNodeInfoService.toleratedTaints[taint.Key]
		if ok {
			result = append(result, taint)
		}
	}
	return result
}

func nodeGroupId(taints []v1.Taint) string {
	idStrings := []string{}
	for _, taint := range taints {
		idStrings = append(idStrings, taint.Key)
	}
	sort.Strings(idStrings)
	return strings.Join(idStrings, ",")
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GetAllAvailableProcessingNodes() ([]*v1.Node, error) {
	allNodes, err := kubernetesNodeInfoService.clusterContext.GetNodes()
	if err != nil {
		return []*v1.Node{}, err
	}

	return util2.FilterNodes(allNodes, kubernetesNodeInfoService.IsAvailableProcessingNode), nil
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GetAllNodes() ([]*v1.Node, error) {
	return kubernetesNodeInfoService.clusterContext.GetNodes()
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) IsAvailableProcessingNode(node *v1.Node) bool {
	if node.Spec.Unschedulable {
		return false
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule &&
			!kubernetesNodeInfoService.toleratedTaints[taint.Key] {
			return false
		}
	}

	return true
}
