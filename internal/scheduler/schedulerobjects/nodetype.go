package schedulerobjects

import (
	"strings"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

type (
	taintsFilterFunc func(*v1.Taint) bool
	labelsFilterFunc func(key, value string) bool
)

func NewNodeTypeFromNode(node *v1.Node, indexedTaints map[string]interface{}, indexedLabels map[string]interface{}) *NodeType {
	return NewNodeType(node.Spec.Taints, node.GetLabels(), indexedTaints, indexedLabels)
}

func NewNodeType(taints []v1.Taint, labels map[string]string, indexedTaints map[string]interface{}, indexedLabels map[string]interface{}) *NodeType {
	if taints == nil {
		taints = make([]v1.Taint, 0)
	}
	if labels == nil {
		labels = make(map[string]string)
	}

	// Filter out any taints that should not be indexed.
	if indexedTaints != nil {
		taints = getFilteredTaints(taints, func(t *v1.Taint) bool {
			_, ok := indexedTaints[t.Key]
			return ok
		})
	} else {
		// The default is to index all taints.
		taints = slices.Clone(taints)
	}

	// Sort taints to ensure node type id is consistent regardless of
	// the order in which taints are set on the node.
	slices.SortFunc(taints, func(a, b v1.Taint) bool { return a.Key < b.Key })

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
	unsetIndexedLabels := make(map[string]string)
	for key := range indexedLabels {
		if _, ok := labels[key]; !ok {
			unsetIndexedLabels[key] = "" // Only the key is used.
		}
	}

	return &NodeType{
		Id:                 nodeTypeIdFromTaintsAndLabels(taints, labels, unsetIndexedLabels),
		Taints:             taints,
		Labels:             labels,
		UnsetIndexedLabels: unsetIndexedLabels,
	}
}

// nodeTypeIdFromTaintsAndLabels generates an id that is unique for each combination
// of taints, labels, and unset labels, of the form
// $taint1$taint2...&$label1=labelValue1$label2=labelValue2...&$unsetIndexedLabel1=unsetIndexedLabelValue1...
//
// We separate taints/labels by $, labels and values by =, and groups by &,
// since these characters are not allowed in taints and labels; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
func nodeTypeIdFromTaintsAndLabels(taints []v1.Taint, labels, unsetIndexedLabels map[string]string) string {
	// TODO: To reduce key size (and thus improve performance), we could hash the string.
	// TODO: We should test this function to ensure there are no collisions. And that the string is never empty.
	var sb strings.Builder
	for _, taint := range taints {
		sb.WriteString("$")
		sb.WriteString(taint.String())
	}
	sb.WriteString("&")
	ls := maps.Keys(labels)
	slices.Sort(ls)
	for _, label := range ls {
		value := labels[label]
		sb.WriteString("$")
		sb.WriteString(label)
		sb.WriteString("=")
		sb.WriteString(value)
	}
	sb.WriteString("&")
	ls = maps.Keys(unsetIndexedLabels)
	slices.Sort(ls)
	for _, label := range ls {
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
	var filteredTaints []v1.Taint
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

func (nodeType *NodeType) DeepCopy() *NodeType {
	if nodeType == nil {
		return nil
	}
	return &NodeType{
		Id:                 nodeType.Id,
		Taints:             slices.Clone(nodeType.Taints),
		Labels:             maps.Clone(nodeType.Labels),
		UnsetIndexedLabels: maps.Clone(nodeType.UnsetIndexedLabels),
	}
}
