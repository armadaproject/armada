package util

import v1 "k8s.io/api/core/v1"

func GetPodsOnNodes(pods []*v1.Pod, nodes []*v1.Node) []*v1.Pod {
	nodeSet := make(map[string]*v1.Node)
	for _, node := range nodes {
		nodeSet[node.Name] = node
	}

	podsOnNodes := []*v1.Pod{}

	for _, pod := range pods {
		if _, presentOnProcessingNode := nodeSet[pod.Spec.NodeName]; presentOnProcessingNode {
			podsOnNodes = append(podsOnNodes, pod)
		}
	}

	return podsOnNodes
}

func FilterNodes(nodes []*v1.Node, filter func(node *v1.Node) bool) []*v1.Node {
	processingNodes := make([]*v1.Node, 0, len(nodes))

	for _, node := range nodes {
		if filter(node) {
			processingNodes = append(processingNodes, node)
		}
	}

	return processingNodes
}

func IsAvailableProcessingNode(node *v1.Node, toleratedTaints map[string]bool) bool {
	if node.Spec.Unschedulable {
		return false
	}

	for _, taint := range node.Spec.Taints {
		if taint.Effect == v1.TaintEffectNoSchedule &&
			!toleratedTaints[taint.Key] {
			return false
		}
	}

	return true
}
