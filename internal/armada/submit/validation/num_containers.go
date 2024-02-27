package validation

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
)

type numContainersValidator struct {
	podSpecValidator
}

func (p numContainersValidator) validatePodSpec(spec *v1.PodSpec) error {
	if len(spec.Containers) == 0 {
		return fmt.Errorf("pod spec has no containers")
	}
	return nil
}
