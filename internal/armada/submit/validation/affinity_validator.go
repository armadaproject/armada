package validation

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"github.com/armadaproject/armada/pkg/api"
)

type affinityValidator struct{}

func (p affinityValidator) Validate(j *api.JobSubmitRequestItem) error {
	affinity := extractNodeAffinity(j)

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
}

func extractNodeAffinity(j *api.JobSubmitRequestItem) *v1.NodeAffinity {
	podSpec := j.GetMainPodSpec()
	if podSpec == nil {
		return nil
	}
	affinity := podSpec.Affinity
	if affinity == nil {
		return nil
	}
	return affinity.NodeAffinity
}
