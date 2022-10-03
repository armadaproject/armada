package schedulerobjects

import (
	"strings"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/pkg/api"
)

type (
	taintsFilterFunc func(*v1.Taint) bool
	labelsFilterFunc func(key, value string) bool
)

func NewNodeTypeFromNode(node *v1.Node, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	return NewNodeType(node.Spec.Taints, node.GetLabels(), taintsFilter, labelsFilter)
}

func NewNodeTypeFromNodeInfo(nodeInfo *api.NodeInfo, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	return NewNodeType(nodeInfo.GetTaints(), nodeInfo.GetLabels(), taintsFilter, labelsFilter)
}

func NewNodeType(taints []v1.Taint, labels map[string]string, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	taints = getFilteredTaints(taints, taintsFilter)
	labels = getFilteredLabels(labels, labelsFilter)
	return &NodeType{
		Id:     nodeTypeIdFromTaintsAndLabels(taints, labels),
		Taints: taints,
		Labels: labels,
	}
}

func nodeTypeIdFromTaintsAndLabels(taints []v1.Taint, labels map[string]string) string {
	var sb strings.Builder
	for _, taint := range taints {
		sb.WriteString(taint.String())
	}
	for label, value := range labels {
		sb.WriteString(label + "=" + value)
	}
	return sb.String()
}

// getFilteredTaints returns a list of taints satisfying the filter predicate.
func getFilteredTaints(taints []v1.Taint, inclusionFilter taintsFilterFunc) []v1.Taint {
	if inclusionFilter == nil {
		return taints
	}
	filteredTaints := []v1.Taint{}
	for _, taint := range taints {
		if !inclusionFilter(&taint) {
			continue
		}
		filteredTaints = append(filteredTaints, taint)
	}
	return filteredTaints
}

// getFilteredLabels returns a map of labels satisfying the filter predicate.
func getFilteredLabels(labels map[string]string, inclusionFilter labelsFilterFunc) map[string]string {
	if inclusionFilter == nil {
		return labels
	}
	filteredLabels := make(map[string]string)
	for key, value := range labels {
		if !inclusionFilter(key, value) {
			continue
		}
		filteredLabels[key] = value
	}
	return filteredLabels
}

// PodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
func (nodeType *NodeType) PodRequirementsMet(req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	// TODO: This check shouldn't reject due to missing labels.
	return PodRequirementsMet(nodeType.Taints, nodeType.Labels, req)
}
