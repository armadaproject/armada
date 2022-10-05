package schedulerobjects

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

type PodRequirementsNotMetReason interface {
	fmt.Stringer
}

type UntoleratedTaint struct {
	Taint v1.Taint
}

func (r *UntoleratedTaint) String() string {
	return fmt.Sprintf("taint %s not tolerated", r.Taint.String())
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
	return fmt.Sprintf(
		"pod requires %s %s, but only %s is available",
		err.Required.String(),
		err.Resource,
		err.Available.String(),
	)
}

// PodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
func (nodeType *NodeType) PodRequirementsMet(req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {

	// TODO: This check shouldn't reject due to missing labels.
	return PodRequirementsMet(nodeType.Taints, nodeType.Labels, req)
}

// PodRequirementsMet returns true if the scheduling requirements in req
// are met by a node with the provided taints and labels.
// If the requirements are not met, it returns the reason for why.
// If the requirements can't be parsed, an error is returned.
func PodRequirementsMet(nodeTaints []v1.Taint, nodeLabels map[string]string, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	untoleratedTaint, hasUntoleratedTaint := corev1.FindMatchingUntoleratedTaint(
		nodeTaints,
		req.Tolerations,
		nil,
	)
	if hasUntoleratedTaint {
		return false, &UntoleratedTaint{Taint: untoleratedTaint}, nil
	}

	for label, podValue := range req.NodeSelector {
		if nodeValue, ok := nodeLabels[label]; ok {
			if nodeValue != podValue {
				return false, &UnmatchedLabel{
					Label:     label,
					PodValue:  podValue,
					NodeValue: nodeValue,
				}, nil
			}
		} else {
			return false, &MissingLabel{Label: label}, nil
		}
	}

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

// PodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
func (nodeType *NodeType) PodRequirementsMet2(req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	matches, reason, err := podTolerationRequirementsMet(nodeType.GetTaints(), req)
	if !matches || err != nil {
		return matches, reason, err
	}
	return podNodeSelectorRequirementsMet(nodeType.GetLabels(), nodeType.GetUnsetWellKnownLabels(), req)
}

// PodRequirementsMet determines whether a pod can be scheduled on this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
func (node *Node) PodRequirementsMet2(req *PodRequirements, assignedResources AssignedByPriorityAndResourceType) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := podTolerationRequirementsMet(node.GetTaints(), req)
	if !matches || err != nil {
		return matches, 0, reason, err
	}

	matches, reason, err = podNodeSelectorRequirementsMet(node.GetLabels(), nil, req)
	if !matches || err != nil {
		return matches, 0, reason, err
	}

	// Check if the pod can be scheduled without preemption,
	// by checking if resource requirements are met at priority 0.
	matches, reason, err = podResourceRequirementsMet(0, node.AvailableByPriorityAndResource, assignedResources, req)
	if matches || err != nil {
		return matches, 1, reason, err
	}

	// Check if the pod can be scheduled with preemption.
	matches, reason, err = podResourceRequirementsMet(req.GetPriority(), node.AvailableByPriorityAndResource, assignedResources, req)
	return matches, 0, reason, err
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

func podNodeSelectorRequirementsMet(nodeLabels, unsetWellKnownLabels map[string]string, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
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
			// If unsetWellKnownLabels is provided, return false only if
			// this label is explicitly marked as not set.
			//
			// If unsetWellKnownLabels is not provided,
			// we assume that nodeLabels contains all labels and return false.
			if unsetWellKnownLabels != nil {
				if _, ok := unsetWellKnownLabels[label]; ok {
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

func podResourceRequirementsMet(priority int32, availableResources AvailableByPriorityAndResourceType, assignedResources AssignedByPriorityAndResourceType, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	available := resource.Quantity{}
	for resource, required := range req.ResourceRequirements.Requests {
		q := availableResources.Get(req.Priority, string(resource))
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(req.Priority, string(resource)))
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
