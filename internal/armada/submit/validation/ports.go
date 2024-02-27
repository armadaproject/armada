package validation

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
)

type portsValidator struct {
	podSpecValidator
}

func (p portsValidator) validatePodSpec(spec *v1.PodSpec) error {
	existingPortSet := make(map[int32]int)
	for index, container := range spec.Containers {
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
