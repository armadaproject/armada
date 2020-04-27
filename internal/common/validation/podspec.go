package validation

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

func ValidatePodSpec(spec *v1.PodSpec) error {
	if spec == nil {
		return fmt.Errorf("empty pod spec")
	}

	if len(spec.Containers) == 0 {
		return fmt.Errorf("pod spec have no containers")
	}

	for _, container := range spec.Containers {
		if len(container.Resources.Limits) == 0 {
			return fmt.Errorf("container %v have no resource limits specified", container.Name)
		}
		if len(container.Resources.Requests) == 0 {
			return fmt.Errorf("container %v have no resource requests specified", container.Name)
		}

		if !resourceListEquals(container.Resources.Requests, container.Resources.Limits) {
			return fmt.Errorf("container %v does not havee resource request and limit equal (this is currentlz not supported)", container.Name)
		}
	}
	return nil
}

func resourceListEquals(a v1.ResourceList, b v1.ResourceList) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if v != b[k] {
			return false
		}
	}
	return true
}
