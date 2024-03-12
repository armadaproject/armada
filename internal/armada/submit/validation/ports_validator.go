package validation

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

type portsValidator struct{}

func (p portsValidator) Validate(j *api.JobSubmitRequestItem) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}

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
