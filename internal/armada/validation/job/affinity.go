package job

import (
	"fmt"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

type AffinityValidator struct{}

func (p AffinityValidator) Validate(j *api.JobSubmitRequestItem) error {

	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		affinity := extractNodeAffinity(spec)
		if affinity == nil {
			return nil // No affinity to check
		}

		// We don't support PreferredDuringSchedulingIgnoredDuringExecution
		if len(affinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
			return fmt.Errorf("preferredDuringSchedulingIgnoredDuringExecution node affinity is not supported by Armada")
		}

		// Check that RequiredDuringSchedulingIgnoredDuringExecution is actually a valid affinity rule
		if affinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			_, err := nodeaffinity.NewNodeSelector(affinity.RequiredDuringSchedulingIgnoredDuringExecution)
			if err != nil {
				return fmt.Errorf("invalid RequiredDuringSchedulingIgnoredDuringExecution node affinity: %v", err)
			}
		}
		return nil
	})
}

func extractNodeAffinity(p *v1.PodSpec) *v1.NodeAffinity {
	affinity := p.Affinity
	if affinity == nil {
		return nil
	}
	return affinity.NodeAffinity
}