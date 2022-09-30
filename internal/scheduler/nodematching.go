package scheduler

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"
)

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

type PodSchedulingRequirements struct {
	Priority             int32
	ResourceRequirements map[string]resource.Quantity
	Tolerations          []v1.Toleration
	NodeSelector         *v1.NodeSelector
}

// canSchedulePod determines whether a pod can be scheduled on this node.
// If the pod can't be scheduled on this node, the returned error indicates why.
// If no error is returned, the pod can be scheduled on this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
func (node *SchedulerNode) canSchedulePod(req *PodSchedulingRequirements, assignedResources AssignedByPriorityAndResourceType) (int, error) {
	err := req.toleratesTaintsAndMatchesSelector(node.GetTaints(), node.GetLabels())
	if err != nil {
		return 0, err
	}

	// Check if the pod can be scheduled without preemption.
	canSchedule := true
	available := resource.Quantity{}
	for resource, required := range req.ResourceRequirements {
		q := node.availableQuantityByPriorityAndResource(0, resource)
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(0, resource))
		if required.Cmp(available) == 1 {
			canSchedule = false
			break
		}
	}
	if canSchedule {
		return 1, nil
	}

	// Check if the pod can be scheduled with preemption.
	for resource, required := range req.ResourceRequirements {
		q := node.availableQuantityByPriorityAndResource(req.Priority, resource)
		q.DeepCopyInto(&available)
		available.Sub(assignedResources.Get(req.Priority, resource))
		if required.Cmp(available) == 1 {
			return 0, &ErrInsufficientResources{
				Resource:  resource,
				Required:  required,
				Available: available,
			}
		}
	}

	return 0, nil
}

// TODO: Return something other than an error if the pod can't be scheduled.
// TODO: For the node selector check, return the constraint that wasn't met.
func (req *PodSchedulingRequirements) toleratesTaintsAndMatchesSelector(taints []v1.Taint, labels map[string]string) error {
	untoleratedTaint, hasUntoleratedTaint := corev1.FindMatchingUntoleratedTaint(
		taints,
		req.Tolerations,
		nil,
	)
	if hasUntoleratedTaint {
		return &ErrUntoleratedTaint{
			Taint: untoleratedTaint,
		}
	}

	if req.NodeSelector != nil {
		matchesNodeSelector, err := corev1.MatchNodeSelectorTerms(
			&v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
			},
			req.NodeSelector,
		)
		if err != nil {
			return err
		}
		if !matchesNodeSelector {
			return &ErrUnmatchedNodeSelector{
				NodeSelector: req.NodeSelector,
			}
		}
	}

	return nil
}
