package node

import (
	"sort"
	"strings"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

type NodeGroupInfoService interface {
	GroupNodesByType(nodes []*v1.Node) []*NodeGroup
	GetType(node *v1.Node) *api.NodeTypeIdentifier
}

type KubernetesNodeInfoService struct {
	clusterPool     string
	toleratedTaints map[string]bool
}

func NewClusterUtilisationService(clusterPool string, toleratedTaints []string) *KubernetesNodeInfoService {
	return &KubernetesNodeInfoService{
		clusterPool:     clusterPool,
		toleratedTaints: util.StringListToSet(toleratedTaints),
	}
}

type NodeGroup struct {
	NodeType *api.NodeTypeIdentifier
	Nodes    []*v1.Node
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GroupNodesByType(nodes []*v1.Node) []*NodeGroup {
	nodeGroupMap := map[string]*NodeGroup{}

	for _, node := range nodes {
		nodeType := kubernetesNodeInfoService.GetType(node)
		if _, present := nodeGroupMap[nodeType.Id]; !present {
			nodeGroupMap[nodeType.Id] = &NodeGroup{
				NodeType: nodeType,
				Nodes:    []*v1.Node{},
			}
		}
		nodeGroupMap[nodeType.Id].Nodes = append(nodeGroupMap[nodeType.Id].Nodes, node)
	}

	nodeGroups := make([]*NodeGroup, 0, len(nodeGroupMap))
	for _, group := range nodeGroupMap {
		nodeGroups = append(nodeGroups, group)
	}

	return nodeGroups
}

func (kubernetesNodeInfoService *KubernetesNodeInfoService) GetType(node *v1.Node) *api.NodeTypeIdentifier {
	groupId := kubernetesNodeInfoService.clusterPool
	relevantTaints := kubernetesNodeInfoService.filterToleratedTaints(node.Spec.Taints)
	if len(relevantTaints) > 0 {
		groupId = nodeGroupId(relevantTaints)
	}

	return &api.NodeTypeIdentifier{
		Id:     groupId,
		Taints: relevantTaints,
	}
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
