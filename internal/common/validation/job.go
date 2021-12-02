package validation

import (
	"fmt"

	"github.com/G-Research/armada/pkg/api"
)

type JobSubmitRequestItemFn func(item *api.JobSubmitRequestItem) error

func JobSubmitRequestItem(maxPodSize uint) JobSubmitRequestItemFn {
	return func(item *api.JobSubmitRequestItem) error {
		if item.PodSpec != nil && len(item.PodSpecs) > 0 {
			return fmt.Errorf("has both pod spec and pod spec list specified")
		}

		if len(item.GetAllPodSpecs()) == 0 {
			return fmt.Errorf("has no pod spec")
		}

		for j, podSpec := range item.GetAllPodSpecs() {
			if err := ValidatePodSpec(podSpec, maxPodSize); err != nil {
				return fmt.Errorf("pod spec with index: %v: %v, %s", j, podSpec, err)
			}
		}

		return validateIngressConfigs(item)
	}
}

func ValidateJobSubmitRequestItem(request *api.JobSubmitRequestItem) error {
	return validateIngressConfigs(request)
}

func validateIngressConfigs(item *api.JobSubmitRequestItem) error {
	existingPortSet := make(map[uint32]int)

	for index, portConfig := range item.Ingress {
		for _, port := range portConfig.Ports {
			if existingIndex, existing := existingPortSet[port]; existing {
				return fmt.Errorf("port %d has two ingress configurations, specified in ingress configs with indexes %d, %d. Each port should at maximum have one ingress configuration",
					port, existingIndex, index)
			} else {
				existingPortSet[port] = index
			}
		}
	}
	return nil
}
