package scheduler

// import (
// 	v1 "k8s.io/api/core/v1"

// 	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
// 	"github.com/G-Research/armada/pkg/api"
// )

// type (
// 	taintsFilterFunc func(*v1.Taint) bool
// 	labelsFilterFunc func(key, value string) bool
// )

// // NodeType represents a particular combination of taints and labels.
// // The scheduler groups nodes by node type. When assigning pods to nodes,
// // the scheduler only considers nodes with a NodeType for which the taints and labels match.
// type NodeType struct {
// 	// Unique identifier of this node type.
// 	// Used for map lookup.
// 	id     string
// 	Taints []v1.Taint
// 	Labels map[string]string
// }

// func NewNodeTypeFromNode(node *v1.Node, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
// 	return NewNodeType(node.Spec.Taints, node.GetLabels(), taintsFilter, labelsFilter)
// }

// func NewNodeTypeFromNodeInfo(nodeInfo *api.NodeInfo, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
// 	return NewNodeType(nodeInfo.GetTaints(), nodeInfo.GetLabels(), taintsFilter, labelsFilter)
// }

// func NewNodeType(taints []v1.Taint, labels map[string]string, taintsFilter taintsFilterFunc, labelsFilter labelsFilterFunc) *NodeType {
// 	return &NodeType{
// 		Taints: getFilteredTaints(taints, taintsFilter),
// 		Labels: getFilteredLabels(labels, labelsFilter),
// 	}
// }

// // getFilteredTaints returns a list of taints satisfying the filter predicate.
// func getFilteredTaints(taints []v1.Taint, inclusionFilter taintsFilterFunc) []v1.Taint {
// 	if inclusionFilter == nil {
// 		return taints
// 	}
// 	filteredTaints := []v1.Taint{}
// 	for _, taint := range taints {
// 		if !inclusionFilter(&taint) {
// 			continue
// 		}
// 		filteredTaints = append(filteredTaints, taint)
// 	}
// 	return filteredTaints
// }

// // getFilteredLabels returns a map of labels satisfying the filter predicate.
// func getFilteredLabels(labels map[string]string, inclusionFilter labelsFilterFunc) map[string]string {
// 	if inclusionFilter == nil {
// 		return labels
// 	}
// 	filteredLabels := make(map[string]string)
// 	for key, value := range labels {
// 		if !inclusionFilter(key, value) {
// 			continue
// 		}
// 		filteredLabels[key] = value
// 	}
// 	return filteredLabels
// }

// // PodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
// func (nodeType *NodeType) PodRequirementsMet(req *schedulerobjects.PodRequirements) (bool, PodRequirementsNotMetReason, error) {
// 	return PodRequirementsMet(nodeType.Taints, nodeType.Labels, req)
// }
