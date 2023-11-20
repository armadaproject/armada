package nodedb

import (
	"fmt"

	"github.com/segmentio/fasthash/fnv1a"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/scheduling/corev1"

	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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
	ResourceName string
	Required     resource.Quantity
	Available    resource.Quantity
}

func (r *InsufficientResources) Sum64() uint64 {
	h := fnv1a.Init64
	h = fnv1a.AddString64(h, r.ResourceName)
	h = fnv1a.AddUint64(h, uint64(r.Required.MilliValue()))
	h = fnv1a.AddUint64(h, uint64(r.Available.MilliValue()))
	return h
}

func (err *InsufficientResources) String() string {
	return "pod requires " + err.Required.String() + " " + err.ResourceName + ", but only " +
		err.Available.String() + " is available"
}

// NodeTypeJobRequirementsMet determines whether a pod can be scheduled on nodes of this NodeType.
// If the requirements are not met, it returns the reason for why.
// If the requirements can't be parsed, an error is returned.
func NodeTypeJobRequirementsMet(nodeType *schedulerobjects.NodeType, jctx *schedulercontext.JobSchedulingContext) (bool, PodRequirementsNotMetReason) {
	matches, reason := TolerationRequirementsMet(nodeType.GetTaints(), jctx.AdditionalTolerations, jctx.PodRequirements.GetTolerations())
	if !matches {
		return matches, reason
	}

	matches, reason = NodeSelectorRequirementsMet(nodeType.GetLabels(), nodeType.GetUnsetIndexedLabels(), jctx.AdditionalNodeSelectors)
	if !matches {
		return matches, reason
	}

	return NodeSelectorRequirementsMet(nodeType.GetLabels(), nodeType.GetUnsetIndexedLabels(), jctx.PodRequirements.GetNodeSelector())
}

// JobRequirementsMet determines whether a job can be scheduled onto this node.
// If the pod can be scheduled, the returned score indicates how well the node fits:
// - 0: Pod can be scheduled by preempting running pods.
// - 1: Pod can be scheduled without preempting any running pods.
// If the requirements are not met, it returns the reason why.
// If the requirements can't be parsed, an error is returned.
func JobRequirementsMet(taints []v1.Taint, labels map[string]string, totalResources schedulerobjects.ResourceList, allocatableResources schedulerobjects.ResourceList, jctx *schedulercontext.JobSchedulingContext) (bool, int, PodRequirementsNotMetReason, error) {
	matches, reason, err := StaticJobRequirementsMet(taints, labels, totalResources, jctx)
	if !matches || err != nil {
		return matches, 0, reason, err
	}
	matches, score, reason := DynamicJobRequirementsMet(allocatableResources, jctx)
	if !matches {
		return matches, 0, reason, nil
	}
	return true, score, nil, nil
}

// StaticJobRequirementsMet checks if a job can be scheduled onto this node,
// accounting for taints, node selectors, node affinity, and total resources available on the node.
func StaticJobRequirementsMet(taints []v1.Taint, labels map[string]string, totalResources schedulerobjects.ResourceList, jctx *schedulercontext.JobSchedulingContext) (bool, PodRequirementsNotMetReason, error) {
	matches, reason := TolerationRequirementsMet(taints, jctx.AdditionalTolerations, jctx.PodRequirements.GetTolerations())
	if !matches {
		return matches, reason, nil
	}

	matches, reason = NodeSelectorRequirementsMet(labels, nil, jctx.AdditionalNodeSelectors)
	if !matches {
		return matches, reason, nil
	}

	matches, reason = NodeSelectorRequirementsMet(labels, nil, jctx.PodRequirements.GetNodeSelector())
	if !matches {
		return matches, reason, nil
	}

	matches, reason, err := NodeAffinityRequirementsMet(labels, jctx.PodRequirements.GetAffinityNodeSelector())
	if !matches || err != nil {
		return matches, reason, err
	}

	matches, reason = ResourceRequirementsMet(totalResources, jctx.PodRequirements.ResourceRequirements.Requests)
	if !matches {
		return matches, reason, nil
	}

	return true, nil, nil
}

// DynamicJobRequirementsMet checks if a pod can be scheduled onto this node,
// accounting for resources allocated to pods already assigned to this node.
func DynamicJobRequirementsMet(allocatableResources schedulerobjects.ResourceList, jctx *schedulercontext.JobSchedulingContext) (bool, int, PodRequirementsNotMetReason) {
	matches, reason := ResourceRequirementsMet(allocatableResources, jctx.PodRequirements.ResourceRequirements.Requests)
	return matches, SchedulableScore, reason
}

func TolerationRequirementsMet(taints []v1.Taint, tolerations ...[]v1.Toleration) (bool, PodRequirementsNotMetReason) {
	untoleratedTaint, hasUntoleratedTaint := findMatchingUntoleratedTaint(taints, tolerations...)
	if hasUntoleratedTaint {
		return false, &UntoleratedTaint{Taint: untoleratedTaint}
	}
	return true, nil
}

// findMatchingUntoleratedTaint checks if the given tolerations tolerates
// all the taints, and returns the first taint without a toleration.
// Returns true if there is an untolerated taint.
// Returns false if all taints are tolerated.
func findMatchingUntoleratedTaint(taints []v1.Taint, tolerations ...[]v1.Toleration) (v1.Taint, bool) {
	for _, taint := range taints {
		taintTolerated := false
		for _, ts := range tolerations {
			taintTolerated = taintTolerated || corev1.TolerationsTolerateTaint(ts, &taint)
			if taintTolerated {
				break
			}
		}
		if !taintTolerated {
			return taint, true
		}
	}
	return v1.Taint{}, false
}

func NodeSelectorRequirementsMet(nodeLabels, unsetIndexedLabels, nodeSelector map[string]string) (bool, PodRequirementsNotMetReason) {
	for label, podValue := range nodeSelector {
		// If the label value differs between nodeLabels and the pod, always return false.
		if nodeValue, ok := nodeLabels[label]; ok {
			if nodeValue != podValue {
				return false, &UnmatchedLabel{
					Label:     label,
					PodValue:  podValue,
					NodeValue: nodeValue,
				}
			}
		} else {
			// If unsetIndexedLabels is provided, return false only if this label is explicitly marked as not set.
			//
			// If unsetIndexedLabels is not provided, we assume that nodeLabels contains all labels and return false.
			if unsetIndexedLabels != nil {
				if _, ok := unsetIndexedLabels[label]; ok {
					return false, &MissingLabel{Label: label}
				}
			} else {
				return false, &MissingLabel{Label: label}
			}
		}
	}
	return true, nil
}

func NodeAffinityRequirementsMet(nodeLabels map[string]string, nodeSelector *v1.NodeSelector) (bool, PodRequirementsNotMetReason, error) {
	if nodeSelector != nil {
		matchesNodeSelector, err := corev1.MatchNodeSelectorTerms(
			&v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Labels: nodeLabels,
				},
			},
			nodeSelector,
		)
		if err != nil {
			return false, nil, err
		}
		if !matchesNodeSelector {
			return false, &UnmatchedNodeSelector{
				NodeSelector: nodeSelector,
			}, nil
		}
	}
	return true, nil, nil
}

func ResourceRequirementsMet(available schedulerobjects.ResourceList, required v1.ResourceList) (bool, PodRequirementsNotMetReason) {
	resourceName, availableQuantity, requiredQuantity, hasGreaterResource := findGreaterQuantity(available, required)
	if hasGreaterResource {
		return false, &InsufficientResources{
			ResourceName: resourceName,
			Required:     requiredQuantity,
			Available:    availableQuantity,
		}
	}
	return true, nil
}

// findGreaterQuantity returns the name of a resource in required with non-zero quantity such that
// the corresponding quantity in available is smaller, or returns false if no such resource can be found.
func findGreaterQuantity(available schedulerobjects.ResourceList, required v1.ResourceList) (string, resource.Quantity, resource.Quantity, bool) {
	for t, requiredQuantity := range required {
		availableQuantity := available.Get(string(t))
		if requiredQuantity.Cmp(availableQuantity) == 1 {
			return string(t), availableQuantity, requiredQuantity, true
		}
	}
	return "", resource.Quantity{}, resource.Quantity{}, false
}
