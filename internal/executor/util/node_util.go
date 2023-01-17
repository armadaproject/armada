package util

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
)

func GetPodsOnNodes(pods []*v1.Pod, nodes []*v1.Node) []*v1.Pod {
	nodeSet := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeSet[node.Name] = node
	}

	podsOnNodes := []*v1.Pod{}

	for _, pod := range pods {
		if _, presentOnNode := nodeSet[pod.Spec.NodeName]; presentOnNode {
			podsOnNodes = append(podsOnNodes, pod)
		}
	}

	return podsOnNodes
}

func ExtractNodeNames(nodes []*v1.Node) []string {
	nodeNames := make([]string, 0, len(nodes))

	for _, node := range nodes {
		nodeNames = append(nodeNames, node.Name)
	}

	return nodeNames
}

func RemoveNodesFromList(list1 []*v1.Node, list2 []*v1.Node) []*v1.Node {
	nodesToRemove := ExtractNodeNames(list2)
	nodesToRemoveSet := util.StringListToSet(nodesToRemove)
	remainingNodes := []*v1.Node{}

	for _, node := range list1 {
		if !nodesToRemoveSet[node.Name] {
			remainingNodes = append(remainingNodes, node)
		}
	}

	return remainingNodes
}

func MergeNodeList(list1 []*v1.Node, list2 []*v1.Node) []*v1.Node {
	nodeNames := ExtractNodeNames(list1)
	nodeNamesSet := util.StringListToSet(nodeNames)

	allNodes := list1

	for _, node := range list2 {
		if !nodeNamesSet[node.Name] {
			allNodes = append(allNodes, node)
		}
	}

	return allNodes
}

func FilterNodes(nodes []*v1.Node, filter func(node *v1.Node) bool) []*v1.Node {
	filteredNodes := make([]*v1.Node, 0, len(nodes))

	for _, node := range nodes {
		if filter(node) {
			filteredNodes = append(filteredNodes, node)
		}
	}

	return filteredNodes
}

func IsReady(node *v1.Node) bool {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}
