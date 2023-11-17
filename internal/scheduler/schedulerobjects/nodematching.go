package schedulerobjects

import (
	"fmt"

	"github.com/segmentio/fasthash/fnv1a"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
)

const (
	// When checking if a pod fits on a node, this score indicates how well the pods fits.
	// However, all nodes are currently given the same score.
	SchedulableScore                                 = 0
	SchedulableBestScore                             = SchedulableScore
	PodRequirementsNotMetReasonUnmatchedNodeSelector = "node does not match pod NodeAffinity"
	PodRequirementsNotMetReasonUnknown               = "unknown"
	PodRequirementsNotMetReasonInsufficientResources = "insufficient resources available"
)

type PodRequirementsNotMetReason interface {
	fmt.Stringer
	// Returns a 64-bit hash of this reason.
	Sum64() uint64
}

type UntoleratedTaint struct {
	Taint v1.Taint
}

func (r *UntoleratedTaint) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, r.Taint.Key)
	h = fnv1a.AddString64(h, r.Taint.Value)
	h = fnv1a.AddString64(h, string(r.Taint.Effect))
	return h
}

func (r *UntoleratedTaint) String() string {
	return fmt.Sprintf("taint %s=%s:%s not tolerated", r.Taint.Key, r.Taint.Value, r.Taint.Effect)
}

type MissingLabel struct {
	Label string
}

func (r *MissingLabel) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, r.Label)
	return h
}

func (r *MissingLabel) String() string {
	return fmt.Sprintf("node does not match pod NodeSelector: label %s not set", r.Label)
}

type UnmatchedLabel struct {
	Label     string
	PodValue  string
	NodeValue string
}

func (r *UnmatchedLabel) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, r.Label)
	h = fnv1a.AddString64(h, r.PodValue)
	h = fnv1a.AddString64(h, r.NodeValue)
	return h
}

func (r *UnmatchedLabel) String() string {
	return fmt.Sprintf("node does not match pod NodeSelector: required label %s = %s, but node has %s", r.Label, r.PodValue, r.NodeValue)
}

type UnmatchedNodeSelector struct {
	NodeSelector *v1.NodeSelector
}

func (r *UnmatchedNodeSelector) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, PodRequirementsNotMetReasonUnmatchedNodeSelector)
	return h
}

func (err *UnmatchedNodeSelector) String() string {
	return PodRequirementsNotMetReasonUnmatchedNodeSelector
}

type InsufficientResources struct {
	Resource  string
	Required  resource.Quantity
	Available resource.Quantity
}

func (r *InsufficientResources) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, r.Resource)
	h = fnv1a.AddUint64(h, uint64(r.Required.MilliValue()))
	h = fnv1a.AddUint64(h, uint64(r.Available.MilliValue()))
	return h
}

func (err *InsufficientResources) String() string {
	return "pod requires " + err.Required.String() + " " + err.Resource + ", but only " +
		err.Available.String() + " is available"
}

// NodeTypePodRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
// If the requirements are not met, it returns the reason for why.
// If the requirements can't be parsed, an error is returned.
func NodeTypePodRequirementsMet(nodeType *NodeType, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
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
func PodRequirementsMet(taints []v1.Taint, labels map[string]string, totalResources ResourceList, allocatableResources ResourceList, req *PodRequirements) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := StaticPodRequirementsMet(taints, labels, totalResources, req)
	if !matches || err != nil {
		return matches, 0, reason, err
	}
	return DynamicPodRequirementsMet(allocatableResources, req)
}

// StaticPodRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for taints, node selectors, node affinity, and total resources available on the node.
func StaticPodRequirementsMet(taints []v1.Taint, labels map[string]string, totalResources ResourceList, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	matches, reason, err := podTolerationRequirementsMet(taints, req)
	if !matches || err != nil {
		return matches, reason, err
	}

	matches, reason, err = podNodeSelectorRequirementsMet(labels, nil, req)
	if !matches || err != nil {
		return matches, reason, err
	}

	matches, reason, err = podNodeAffinityRequirementsMet(labels, req)
	if !matches || err != nil {
		return matches, reason, err
	}

	for resource, required := range req.ResourceRequirements.Requests {
		available := totalResources.Get(string(resource))
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

// DynamicJobRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for resources allocated to pods already assigned to this node.
func DynamicJobRequirementsMet(allocatableResources ResourceList, jctx *schedulercontext.JobSchedulingContext) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := podResourceRequirementsMet(allocatableResources, jctx.PodRequirements)
	return matches, SchedulableScore, reason, err
}

// DynamicPodRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for resources allocated to pods already assigned to this node.
func DynamicPodRequirementsMet(allocatableResources ResourceList, req *PodRequirements) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := podResourceRequirementsMet(allocatableResources, req)
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

func podResourceRequirementsMet(allocatableResources ResourceList, req *PodRequirements) (bool, PodRequirementsNotMetReason, error) {
	for resource, required := range req.ResourceRequirements.Requests {
		available := allocatableResources.Get(string(resource))
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
