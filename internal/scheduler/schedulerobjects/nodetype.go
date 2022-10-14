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
	// TODO: Pass through well-known labels from config.
	unsetWellKnownLabels := getFilteredLabels(wellKnownLabels, func(key, _ string) bool {
		_, ok := labels[key]
		return !ok
	})
	if unsetWellKnownLabels == nil {
		unsetWellKnownLabels = make(map[string]string)
	}
	taints = getFilteredTaints(taints, taintsFilter)
	labels = getFilteredLabels(labels, labelsFilter)
	return &NodeType{
		Id:                   nodeTypeIdFromTaintsAndLabels(taints, labels, unsetWellKnownLabels),
		Taints:               taints,
		Labels:               labels,
		UnsetWellKnownLabels: unsetWellKnownLabels,
	}
}

func NewNodeTypeNew(taints []v1.Taint, labels map[string]string, indexedLabels map[string]interface{}, indexedTaints map[string]interface{}) *NodeType {

	// Filter out any taints that should not be indexed.
	// The default is to index all taints.
	if indexedTaints != nil {
		labels = getFilteredLabels(labels, func(key, _ string) bool {
			_, ok := indexedLabels[key]
			return ok
		})
	}

	// Filter out any labels that should not be indexed.
	if indexedLabels != nil {
		labels = getFilteredLabels(labels, func(key, _ string) bool {
			_, ok := indexedLabels[key]
			return ok
		})
	} else {
		// The default is to not index any labels.
		labels = make(map[string]string)
	}

	// Get the indexed labels that are not set to create indexes for unset labels.
	setIndexedLabels := make(map[string]string)
	for key, value := range labels {
		setIndexedLabels[key] = value
	}
	unsetIndexedLabels := make(map[string]string)
	for key := range indexedLabels {
		if _, ok := setIndexedLabels[key]; !ok {
			unsetIndexedLabels[key] = "" // Only the key is used.
		}
	}

	return &NodeType{
		Id:                   nodeTypeIdFromTaintsAndLabels(taints, labels, unsetIndexedLabels),
		Taints:               taints,
		Labels:               labels,
		UnsetWellKnownLabels: unsetIndexedLabels,
	}
}

// nodeTypeIdFromTaintsAndLabels generates an id that is unique for each combination
// of taints, labels, and unset labels, of the form
// $taint1$taint2...&$label1=labelValue1$label2=labelValue2...&$unsetWellKnownLabel1=unsetWellKnownLabelValue1...
//
// We separate taints/labels by $, labels and values by =, and and groups by &,
// since these characters are not allowed in taints and labels; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
func nodeTypeIdFromTaintsAndLabels(taints []v1.Taint, labels, unsetWellKnownLabels map[string]string) string {
	// TODO: To reduce key size (and thus improve performance), we could hash the string.
	// TODO: We should test this function to ensure there are no collisions. And that the string is never empty.
	var sb strings.Builder
	for _, taint := range taints {
		sb.WriteString("$")
		sb.WriteString(taint.String())
	}
	sb.WriteString("&")
	for label, value := range labels {
		sb.WriteString("$")
		sb.WriteString(label)
		sb.WriteString("=")
		sb.WriteString(value)
	}
	sb.WriteString("&")
	for label := range unsetWellKnownLabels {
		sb.WriteString("$")
		sb.WriteString(label)
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
