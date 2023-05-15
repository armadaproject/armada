package schedulerobjects

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

const (
	// When checking if a pod fits on a node, this score indicates how well the pods fits.
	// However, all nodes are currently given the same score.
	SchedulableScore     = 0
	SchedulableBestScore = SchedulableScore
)

type PodRequirementsNotMetReason interface {
	fmt.Stringer
}

type UntoleratedTaint struct {
	Taint v1.Taint
}

func (r *UntoleratedTaint) String() string {
	return fmt.Sprintf("taint %s=%s:%s not tolerated", r.Taint.Key, r.Taint.Value, r.Taint.Effect)
}

type MissingLabel struct {
	Label string
}

func (r *MissingLabel) String() string {
	return fmt.Sprintf("node does not match pod NodeSelector: label %s not set", r.Label)
}

type UnmatchedLabel struct {
	Label     string
	PodValue  string
	NodeValue string
}

func (r *UnmatchedLabel) String() string {
	return fmt.Sprintf("node does not match pod NodeSelector: required label %s = %s, but node has %s", r.Label, r.PodValue, r.NodeValue)
}

type UnmatchedNodeSelector struct {
	NodeSelector *v1.NodeSelector
}

func (err *UnmatchedNodeSelector) String() string {
	return "node does not match pod NodeAffinity"
}

type InsufficientResources struct {
	Resource  string
	Required  resource.Quantity
	Available resource.Quantity
}

func (err *InsufficientResources) String() string {
	// Note that the below is much faster than fmt.Sprintf().
	// This is important as this gets called in a tight loop
	return "pod requires " + err.Required.String() + " " + err.Resource + ", but only " +
		err.Available.String() + " is available"

	// TODO: this would be even faster
	// return "insufficient " + err.Resource
}

// PodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
// If the requirements are not met, it returns the reason for why.
// If the requirements can't be parsed, an error is returned.
func (nodeType *NodeType) PodRequirementsMet(req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	matches, reason, err := podTolerationRequirementsMet(nodeType.GetTaints(), req)
	if !matches || err != nil {
		return matches, reason, err
	}
	return podNodeSelectorRequirementsMet(nodeType.GetLabels(), nodeType.GetUnsetIndexedLabels(), req)
}

// PodRequirementsMet determines whether a pod can be scheduled onto this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
// If the requirements are not met, it returns the reason why.
// If the requirements can't be parsed, an error is returned.
func (node *Node) PodRequirementsMet(priority int32, req *PodRequirements) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := node.StaticPodRequirementsMet(req)
	if !matches || err != nil {
		return matches, 0, reason, err
	}
	return node.DynamicPodRequirementsMet(priority, req)
}

// StaticPodRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for taints, node selectors, node affinity, and total resources available on the node.
func (node *Node) StaticPodRequirementsMet(req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	matches, reason, err := podTolerationRequirementsMet(node.GetTaints(), req)
	if !matches || err != nil {
		return matches, reason, err
	}

	matches, reason, err = podNodeSelectorRequirementsMet(node.GetLabels(), nil, req)
	if !matches || err != nil {
		return matches, reason, err
	}

	matches, reason, err = podNodeAffinityRequirementsMet(node.GetLabels(), req)
	if !matches || err != nil {
		return matches, reason, err
	}

	for resource, required := range req.ResourceRequirements.Requests {
		available := node.TotalResources.Get(string(resource))
		if required.Cmp(available) == 1 {
			return false, &InsufficientResources{
				Resource:  string(resource),
				Required:  required,
				Available: available,
			}, nil
		}
	}

	return true, nil, nil
}

// DynamicPodRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for resources allocated to pods already assigned to this node.
func (node *Node) DynamicPodRequirementsMet(priority int32, req *PodRequirements) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := podResourceRequirementsMet(priority, node.AllocatableByPriorityAndResource, req)
	return matches, SchedulableScore, reason, err
}

func podTolerationRequirementsMet(nodeTaints []v1.Taint, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	untoleratedTaint, hasUntoleratedTaint := corev1.FindMatchingUntoleratedTaint(
		nodeTaints,
		req.Tolerations,
		nil,
	)
	if hasUntoleratedTaint {
		return false, &UntoleratedTaint{Taint: untoleratedTaint}, nil
	}
	return true, nil, nil
}

func podNodeSelectorRequirementsMet(nodeLabels, unsetIndexedLabels map[string]string, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	for label, podValue := range req.NodeSelector {
		// If the label value differs between nodeLabels and the pod,
		// always return false.
		if nodeValue, ok := nodeLabels[label]; ok {
			if nodeValue != podValue {
				return false, &UnmatchedLabel{
					Label:     label,
					PodValue:  podValue,
					NodeValue: nodeValue,
				}, nil
			}
		} else {
			// If unsetIndexedLabels is provided, return false only if
			// this label is explicitly marked as not set.
			//
			// If unsetIndexedLabels is not provided,
			// we assume that nodeLabels contains all labels and return false.
			if unsetIndexedLabels != nil {
				if _, ok := unsetIndexedLabels[label]; ok {
					return false, &MissingLabel{Label: label}, nil
				}
			} else {
				return false, &MissingLabel{Label: label}, nil
			}
		}
	}
	return true, nil, nil
}

func podNodeAffinityRequirementsMet(nodeLabels map[string]string, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	if affinityNodeSelector := req.GetAffinityNodeSelector(); affinityNodeSelector != nil {
		matchesNodeSelector, err := corev1.MatchNodeSelectorTerms(
			&v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: nodeLabels,
				},
			},
			affinityNodeSelector,
		)
		if err != nil {
			return false, nil, err
		}
		if !matchesNodeSelector {
			return false, &UnmatchedNodeSelector{
				NodeSelector: affinityNodeSelector,
			}, nil
		}
	}
	return true, nil, nil
}

func podResourceRequirementsMet(priority int32, allocatableResources AllocatableByPriorityAndResourceType, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	for resource, required := range req.ResourceRequirements.Requests {
		available := allocatableResources.Get(priority, string(resource))
		if required.Cmp(available) == 1 {
			return false, &InsufficientResources{
				Resource:  string(resource),
				Required:  required,
				Available: available,
			}, nil
		}
	}
	return true, nil, nil
}
