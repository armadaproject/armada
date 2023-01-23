package scheduling

import (
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/api"
)

type PodMatchingContext struct {
	podSpec                      *v1.PodSpec
	totalPodResourceRequest      armadaresource.ComputeResourcesFloat
	requiredNodeAffinitySelector *nodeaffinity.LazyErrorNodeSelector
}

func NewPodMatchingContext(podSpec *v1.PodSpec) *PodMatchingContext {
	return &PodMatchingContext{
		podSpec:                      podSpec,
		totalPodResourceRequest:      armadaresource.TotalPodResourceRequest(podSpec).AsFloat(),
		requiredNodeAffinitySelector: makeRequiredNodeAffinitySelector(podSpec),
	}
}

func (podCtx *PodMatchingContext) Matches(nodeType *api.NodeType, availableResources armadaresource.ComputeResourcesFloat) (bool, error) {
	if ok, err := fits(podCtx.totalPodResourceRequest, availableResources); !ok {
		return false, err
	}
	if ok, err := matchNodeSelector(podCtx.podSpec, nodeType.Labels); !ok {
		return false, err
	}
	if ok, err := tolerates(podCtx.podSpec, nodeType.Taints); !ok {
		return false, err
	}
	if ok, err := matchesRequiredNodeAffinity(podCtx.requiredNodeAffinitySelector, nodeType); !ok {
		return false, err
	}
	return true, nil
}

// fits returns true if the requested resources are no greater than the available resources.
func fits(resourceRequest, availableResources armadaresource.ComputeResourcesFloat) (bool, error) {
	for resourceType, requestedResourceQuantity := range resourceRequest {
		// Do not return error on requesting zero of some resource.
		if requestedResourceQuantity <= 0 {
			continue
		}
		availableResourceQuantity, ok := availableResources[resourceType]
		if !ok {
			return false, errors.Errorf("pod requested resource %s, but none is available", resourceType)
		}
		if availableResourceQuantity < requestedResourceQuantity {
			return false, errors.Errorf(
				"pod requested %f of resource %s, but only %f is available",
				requestedResourceQuantity,
				resourceType,
				availableResourceQuantity,
			)
		}
	}
	return true, nil
}

// matchNodeSelector returns true if the NodeSelector includes nodes with the provided labels,
// i.e., if the labels set by the NodeSelector matches the given labels.
func matchNodeSelector(podSpec *v1.PodSpec, labels map[string]string) (bool, error) {
	for label, selectorValue := range podSpec.NodeSelector {
		nodeValue, ok := labels[label]
		if labels == nil || !ok {
			return false, errors.Errorf("node selector requires labels[%s] = %s, but the node type does not include this label", label, selectorValue)
		}
		if nodeValue != selectorValue {
			return false, errors.Errorf(
				"node selector requires labels[%s] = %s, found labels[%s] = %s for this node type",
				label,
				selectorValue,
				label,
				nodeValue,
			)
		}
	}
	return true, nil
}

// tolerates returns true if the pod tolerates all the provided taints.
func tolerates(podSpec *v1.PodSpec, taints []v1.Taint) (bool, error) {
	for i, taint := range taints {
		if taint.Effect == v1.TaintEffectPreferNoSchedule {
			continue // Only check hard constraints.
		}
		if !tolerationsTolerateTaint(podSpec.Tolerations, &taints[i]) {
			return false, errors.Errorf("node type has taint %s of value %s not tolerated by the pod", taint.Key, taint.Value)
		}
	}
	return true, nil
}

// https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/core/v1/helper/helpers.go#L427
func tolerationsTolerateTaint(tolerations []v1.Toleration, taint *v1.Taint) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(taint) {
			return true
		}
	}
	return false
}

// Note this only supports MatchExpressions (i.e. label matching), not MatchFields.
func matchesRequiredNodeAffinity(nodeSelector *nodeaffinity.LazyErrorNodeSelector, nodeType *api.NodeType) (bool, error) {
	if nodeSelector == nil {
		return true, nil
	}

	node := &v1.Node{}
	node.Labels = nodeType.Labels
	match, err := nodeSelector.Match(node)
	if !match {
		if err != nil {
			return match, err
		}
		return false, errors.New("node affinity does not match any node selector terms")
	}
	return match, err
}

func makeRequiredNodeAffinitySelector(podSpec *v1.PodSpec) *nodeaffinity.LazyErrorNodeSelector {
	affinity := podSpec.Affinity
	if affinity == nil {
		return nil
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}

	requiredNodeAffinity := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if requiredNodeAffinity == nil {
		return nil
	}

	return nodeaffinity.NewLazyErrorNodeSelector(requiredNodeAffinity)
}
