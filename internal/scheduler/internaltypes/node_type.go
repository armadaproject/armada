package internaltypes

import (
	"github.com/segmentio/fasthash/fnv1a"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	v1 "k8s.io/api/core/v1"

	koTaint "github.com/armadaproject/armada/internal/scheduler/kubernetesobjects/taint"
)

// NodeType represents a particular combination of taints and labels.
// The scheduler groups nodes by node type. When assigning pods to nodes,
// the scheduler only considers nodes with a NodeType for which the taints and labels match.
// Its fields should be immutable! Do not change these!
type NodeType struct {
	// Unique identifier. Used for map lookup.
	id uint64
	// Kubernetes taints.
	// To reduce the number of distinct node types,
	// may contain only a subset of the taints of the node the node type is created from.
	taints []v1.Taint
	// Kubernetes labels.
	// To reduce the number of distinct node types,
	// may contain only a subset of the labels of the node the node type is created from.
	labels map[string]string
	// Well-known labels not set by this node type.
	// Used to filter out nodes when looking for nodes for a pod
	// that requires at least one well-known label to be set.
	unsetIndexedLabels map[string]string
}

func (m *NodeType) GetId() uint64 {
	return m.id
}

func (m *NodeType) GetTaints() []v1.Taint {
	return koTaint.DeepCopyTaints(m.taints)
}

func (m *NodeType) FindMatchingUntoleratedTaint(tolerations ...[]v1.Toleration) (v1.Taint, bool) {
	return koTaint.FindMatchingUntoleratedTaint(m.taints, tolerations...)
}

func (m *NodeType) GetLabels() map[string]string {
	return deepCopyLabels(m.labels)
}

func (m *NodeType) GetLabelValue(key string) (string, bool) {
	val, ok := m.labels[key]
	return val, ok
}

func (m *NodeType) GetUnsetIndexedLabels() map[string]string {
	return deepCopyLabels(m.unsetIndexedLabels)
}

func (m *NodeType) GetUnsetIndexedLabelValue(key string) (string, bool) {
	val, ok := m.unsetIndexedLabels[key]
	return val, ok
}

type (
	taintsFilterFunc func(*v1.Taint) bool
	labelsFilterFunc func(key, value string) bool
)

func NewNodeType(taints []v1.Taint, labels map[string]string, indexedTaints map[string]bool, indexedLabels map[string]bool) *NodeType {
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
	slices.SortFunc(taints, func(a, b v1.Taint) int {
		if a.Key < b.Key {
			return -1
		} else if a.Key > b.Key {
			return 1
		} else {
			return 0
		}
	}) // TODO: Use less ambiguous sorting.

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
		id:                 nodeTypeIdFromTaintsAndLabels(taints, labels, unsetIndexedLabels),
		taints:             taints,
		labels:             labels,
		unsetIndexedLabels: unsetIndexedLabels,
	}
}

// nodeTypeIdFromTaintsAndLabels generates a id unique for each combination of taints, labels, and unset labels.
// The id is based on the fnv1a hash. Hash collisions do not affect correctness, only the efficiency of sorting out nodes.
//
// We separate taints/labels by $, labels and values by =, and and groups by &,
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
