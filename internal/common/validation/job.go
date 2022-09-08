package validation

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/armadaerrors"

	"github.com/G-Research/armada/pkg/api"
)

func ValidateJobRequestItemPodSpec(r *api.JobSubmitRequestItem) error {
	if r.PodSpec == nil && len(r.PodSpecs) == 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   r.PodSpec,
			Message: "Job does not contain at least one PodSpec",
		})
	}

	// We only support jobs with a single PodSpec, and it must be set to r.PodSpec.
	if r.PodSpec == nil && len(r.PodSpecs) == 1 {
		r.PodSpec = r.PodSpecs[0]
		r.PodSpecs = nil
	}

	// I'm not convinced that the code to create services/ingresses when multiple pods are submitted is correct.
	// In particular, I think job.populateServicesIngresses is wrong.
	// Hence, we return an error until we can make sure that the code is correct.
	// The next error is redundant with this one, but we leave both since we may wish to remove this one.
	// - Albin
	if len(r.PodSpecs) > 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpecs",
			Value:   r.PodSpecs,
			Message: "Jobs with multiple pods are not supported",
		})
	}

	// I'm not convinced the code is correct when combining r.PodSpec and r.PodSpecs.
	// We should do more testing to make sure it's safe before we allow it.
	// - Albin
	if len(r.PodSpecs) > 0 && r.PodSpec != nil {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   r.PodSpec,
			Message: "PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)",
		})
	}

	return nil
}

func ValidateJobRequestItemPriorityClass(r *api.JobSubmitRequestItem, preemptionEnabled bool, allowedPriorityClasses map[string]int32) error {
	priorityClassName := r.PodSpec.PriorityClassName
	if priorityClassName != "" {
		if !preemptionEnabled {
			return errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PriorityClassName",
				Value:   r.PodSpec.PriorityClassName,
				Message: "Preemption is disabled in Server config",
			})
		}
		if _, exists := allowedPriorityClasses[priorityClassName]; !exists {
			return errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PriorityClassName",
				Value:   r.PodSpec.PriorityClassName,
				Message: "Specified Priority Class is not supported in Server config",
			})
		}
	}

	return nil
}

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
				return fmt.Errorf(
					"port %d has two ingress configurations, specified in ingress configs with indexes %d, %d. Each port should at maximum have one ingress configuration",
					port,
					existingIndex,
					index,
				)
			} else {
				existingPortSet[port] = index
			}
		}
	}
	return nil
}
