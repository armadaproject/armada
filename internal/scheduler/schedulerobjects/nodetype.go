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

func NewNodeTypeFromNode(node *v1.Node, wellKnownLabels map[string]string, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	return NewNodeType(node.Spec.Taints, node.GetLabels(), wellKnownLabels, taintsFilter, labelsFilter)
}

func NewNodeTypeFromNodeInfo(nodeInfo *api.NodeInfo, wellKnownLabels map[string]string, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	return NewNodeType(nodeInfo.GetTaints(), nodeInfo.GetLabels(), wellKnownLabels, taintsFilter, labelsFilter)
}

func NewNodeType(taints []v1.Taint, labels, wellKnownLabels map[string]string, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
	taints = getFilteredTaints(taints, taintsFilter)
	labels = getFilteredLabels(labels, labelsFilter)
	unsetWellKnownLabels := getFilteredLabels(labels, func(key, _ string) bool {
		_, ok := labels[key]
		return !ok
	})
	return &NodeType{
		Id:                   nodeTypeIdFromTaintsAndLabels(taints, labels, unsetWellKnownLabels),
		Taints:               taints,
		Labels:               labels,
		UnsetWellKnownLabels: unsetWellKnownLabels,
	}
}

// nodeTypeIdFromTaintsAndLabels generates an id that is unique for each combination
// of taints, labels, and unset labels. We insert "$" characters to achieve this,
// since taints and labels are not allowed to contain this character.
// See:
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
func nodeTypeIdFromTaintsAndLabels(taints []v1.Taint, labels, unsetWellKnownLabels map[string]string) string {
	var sb strings.Builder
	for _, taint := range taints {
		sb.WriteString("$t")
		sb.WriteString(taint.String())
	}
	for label, value := range labels {
		sb.WriteString("$l")
		sb.WriteString(label + "=" + value)
	}
	for label, value := range unsetWellKnownLabels {
		sb.WriteString("$u")
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
