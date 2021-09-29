package scheduling

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

type PodMatchingContext struct {
	podSpec                      *v1.PodSpec
	totalPodResourceRequest      common.ComputeResourcesFloat
	requiredNodeAffinitySelector *nodeaffinity.LazyErrorNodeSelector
}

func NewPodMatchingContext(podSpec *v1.PodSpec) *PodMatchingContext {
	return &PodMatchingContext{
		podSpec:                      podSpec,
		totalPodResourceRequest:      common.TotalPodResourceRequest(podSpec).AsFloat(),
		requiredNodeAffinitySelector: makeRequiredNodeAffinitySelector(podSpec),
	}
}

func (podCtx *PodMatchingContext) Matches(nodeType *api.NodeType, availableResources common.ComputeResourcesFloat) bool {
	return fits(podCtx.totalPodResourceRequest, availableResources) && matchNodeSelector(podCtx.podSpec, nodeType.Labels) && tolerates(podCtx.podSpec, nodeType.Taints) && matchesRequiredNodeAffinity(podCtx.requiredNodeAffinitySelector, nodeType)
}

func fits(resourceRequest, availableResources common.ComputeResourcesFloat) bool {
	r := availableResources.DeepCopy()
	r.Sub(resourceRequest)
	return r.IsValid()
}

func matchNodeSelector(podSpec *v1.PodSpec, labels map[string]string) bool {
	for k, v := range podSpec.NodeSelector {
		if labels == nil || labels[k] != v {
			return false
		}
	}
	return true
}

func tolerates(podSpec *v1.PodSpec, taints []v1.Taint) bool {
	for _, taint := range taints {
		// check only hard constraints
		if taint.Effect == v1.TaintEffectPreferNoSchedule {
			continue
		}

		if !tolerationsTolerateTaint(podSpec.Tolerations, &taint) {
			return false
		}
	}
	return true
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

// Note this only supports MatchExpressions (i.e. label matching), not MatchFields
func matchesRequiredNodeAffinity(nodeSelector *nodeaffinity.LazyErrorNodeSelector, nodeType *api.NodeType) bool {

	if nodeSelector == nil {
		return true
	}

	node := &v1.Node{}
	node.Labels = nodeType.Labels

	match, err := nodeSelector.Match(node)
	if err != nil {
		return false
	}

	return match
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
