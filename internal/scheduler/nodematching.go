package scheduler

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"

	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
)

type PodRequirementsNotMetReason interface {
	fmt.Stringer
}

type UntoleratedTaint struct {
	Taint v1.Taint
}

func (r *UntoleratedTaint) String() string {
	return fmt.Sprintf("taint %+v not tolerated", r.Taint)
}

type MissingLabel struct {
	Label string
}

func (r *MissingLabel) String() string {
	return fmt.Sprintf("label %s not set", r.Label)
}

type UnmatchedLabel struct {
	Label     string
	PodValue  string
	NodeValue string
}

func (r *UnmatchedLabel) String() string {
	return fmt.Sprintf("required label %s = %s, but got %s", r.Label, r.PodValue, r.NodeValue)
}

type UnmatchedNodeSelector struct {
	NodeSelector *v1.NodeSelector
}

func (err *UnmatchedNodeSelector) String() string {
	return fmt.Sprintf("node does not match %+v", err.NodeSelector)
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

type ErrUntoleratedTaint struct {
	Taint v1.Taint
}

func (err *ErrUntoleratedTaint) Error() string {
	return fmt.Sprintf("taint %+v not tolerated", err.Taint)
}

type ErrUnmatchedNodeSelector struct {
	NodeSelector *v1.NodeSelector
}

func (err *ErrUnmatchedNodeSelector) Error() string {
	return fmt.Sprintf("node does not match %+v", err.NodeSelector)
}

type ErrInsufficientResources struct {
	Resource  string
	Required  resource.Quantity
	Available resource.Quantity
}

func (err *ErrInsufficientResources) Error() string {
	return fmt.Sprintf(
		"pod requires %s %s, but only %s is available",
		err.Required.String(),
		err.Resource,
		err.Available.String(),
	)
}

// PodRequirementsMet determines whether a pod can be scheduled on this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
func (node *SchedulerNode) PodRequirementsMet(req *schedulerobjects.PodRequirements, assignedResources AssignedByPriorityAndResourceType) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := PodRequirementsMet(node.GetTaints(), node.GetLabels(), req)
	if matches == false || err != nil {
		return matches, 0, reason, err
	}

	// Check if the pod can be scheduled without preemption.
	canSchedule := true
	available := resource.Quantity{}
	for resource, required := range req.ResourceRequirements.Requests {
		q := node.availableQuantityByPriorityAndResource(0, string(resource))
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(0, string(resource)))
		if required.Cmp(available) == 1 {
			canSchedule = false
			break
		}
	}
	if canSchedule {
		return true, 1, nil, nil
	}

	// Check if the pod can be scheduled with preemption.
	for resource, required := range req.ResourceRequirements.Requests {
		q := node.availableQuantityByPriorityAndResource(req.Priority, string(resource))
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(req.Priority, string(resource)))
		if required.Cmp(available) == 1 {
			return false, 0, &InsufficientResources{
				Resource:  string(resource),
				Required:  required,
				Available: available,
			}, nil
		}
	}

	return true, 0, nil, nil
}

// PodRequirementsMet returns true if the scheduling requirements in req
// are met by a node with the provided taints and labels.
// If the requirements are not met, it returns the reason for why.
// If the requirements can't be parsed, an error is returned.
func PodRequirementsMet(nodeTaints []v1.Taint, nodeLabels map[string]string, req *schedulerobjects.PodRequirements) (bool, PodRequirementsNotMetReason, error) {
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
