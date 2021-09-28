package validation

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

func ValidatePodSpec(spec *v1.PodSpec) error {
	if spec == nil {
		return fmt.Errorf("empty pod spec")
	}

	if len(spec.Containers) == 0 {
		return fmt.Errorf("pod spec has no containers")
	}

	err := validateAffinity(spec.Affinity)
	if err != nil {
		return err
	}

	for _, container := range spec.Containers {
		if len(container.Resources.Limits) == 0 {
			return fmt.Errorf("container %v has no resource limits specified", container.Name)
		}
		if len(container.Resources.Requests) == 0 {
			return fmt.Errorf("container %v has no resource requests specified", container.Name)
		}

		if !resourceListEquals(container.Resources.Requests, container.Resources.Limits) {
			return fmt.Errorf("container %v does not have resource request and limit equal (this is currently not supported)", container.Name)
		}
	}
	return validatePorts(spec)
}

func validateAffinity(affinity *v1.Affinity) error {
	if affinity == nil {
		return nil
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}

	err := validatePreferredNodeAffinityNotPresent(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	if err != nil {
		return err
	}

	return validateRequiredNodeAffinity(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
}

func validatePreferredNodeAffinityNotPresent(preferred []v1.PreferredSchedulingTerm) error {
	if preferred != nil && len(preferred) > 0 {
		return errors.New("PreferredDuringSchedulingIgnoredDuringExecution node affinity is not supported by Armada")
	}
	return nil
}

func validateRequiredNodeAffinity(required *v1.NodeSelector) error {
	if required == nil {
		return nil
	}

	_, err := nodeaffinity.NewNodeSelector(required)
	if err != nil {
		return fmt.Errorf("invalid RequiredDuringSchedulingIgnoredDuringExecution node affinity: %v", err)
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

func validatePorts(podSpec *v1.PodSpec) error {
	existingPortSet := make(map[int32]int)
	for index, container := range podSpec.Containers {
		for _, port := range container.Ports {
			if existingIndex, existing := existingPortSet[port.ContainerPort]; existing {
				return fmt.Errorf(
					"container port %d is exposed multiple times, specified in containers with indexes %d, %d. Should only be exposed once",
					port.ContainerPort, existingIndex, index)
			} else {
				existingPortSet[port.ContainerPort] = index
			}
		}
	}
	return nil
}
