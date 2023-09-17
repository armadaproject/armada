package schedulerobjects

import (
	"github.com/segmentio/fasthash/fnv1a"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"
)

type (
	taintsFilterFunc func(*v1.Taint) bool
	labelsFilterFunc func(key, value string) bool
)

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
	slices.SortFunc(taints, func(a, b v1.Taint) bool { return a.Key < b.Key }) // TODO: Use less ambiguous sorting.

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

// nodeTypeIdFromTaintsAndLabels generates a unique id for each combination of taints, labels, and unset labels.
// The id is based on the fnv1a hash. Hash collisions do not affect correctness, only the efficiency of sorting out nodes.
//
// We separate taints/labels by $, labels and values by =,  and groups by &,
// since these characters are not allowed in taints and labels; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
// https://man.archlinux.org/man/community/kubectl/kubectl-taint.1.en
func nodeTypeIdFromTaintsAndLabels(taints []v1.Taint, labels, unsetIndexedLabels map[string]string) uint64 {
	// TODO: We should test this function to ensure there are no collisions. And that the string is never empty.
	h := fnv1a.Init64
	for _, taint := range taints {
		h = fnv1a.AddString64(h, taint.Key)
		h = fnv1a.AddString64(h, "=")
		h = fnv1a.AddString64(h, taint.Value)
		h = fnv1a.AddString64(h, ":")
		h = fnv1a.AddString64(h, string(taint.Effect))
		h = fnv1a.AddString64(h, "$")
	}
	h = fnv1a.AddString64(h, "&")

	ls := maps.Keys(labels)
	slices.Sort(ls)
	for _, label := range ls {
		value := labels[label]
		h = fnv1a.AddString64(h, label)
		h = fnv1a.AddString64(h, "=")
		h = fnv1a.AddString64(h, value)
		h = fnv1a.AddString64(h, "$")
	}
	h = fnv1a.AddString64(h, "&")

	ls = maps.Keys(unsetIndexedLabels)
	slices.Sort(ls)
	for _, label := range ls {
		h = fnv1a.AddString64(h, label)
		h = fnv1a.AddString64(h, "$")
	}
	return h
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
