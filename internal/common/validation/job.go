package validation

import (
	"fmt"

	"github.com/G-Research/armada/pkg/api"
)

func ValidateJobSubmitRequestItem(request *api.JobSubmitRequestItem) error {
	return validateIngressConfigs(request)
}

func validateIngressConfigs(item *api.JobSubmitRequestItem) error {
	existingPortSet := make(map[uint32]int)

	for index, portConfig := range item.Ingress {
		if len(portConfig.Ports) == 0 {
			return fmt.Errorf("ingress contains zero ports. Each ingress should have at least one port.")
		}

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
