package validation

import (
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/armada/configuration"

	"github.com/G-Research/armada/internal/common/armadaerrors"

	"github.com/G-Research/armada/pkg/api"
)

func ValidateApiJob(j *api.Job, config configuration.PreemptionConfig) error {
	if err := ValidateApiJobPodSpecs(j); err != nil {
		return err
	}
	return ValidatePodSpecPriorityClass(j.PodSpec, config.Enabled, config.PriorityClasses)
}

func ValidateApiJobPodSpecs(j *api.Job) error {
	if j.PodSpec == nil && len(j.PodSpecs) == 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   j.PodSpec,
			Message: "Job does not contain at least one PodSpec",
		})
	}

	// We only support jobs with a single PodSpec, and it must be set to j.PodSpec.
	if j.PodSpec == nil && len(j.PodSpecs) == 1 {
		j.PodSpec = j.PodSpecs[0]
		j.PodSpecs = nil
	}

	// I'm not convinced that the code to create services/ingresses when multiple pods are submitted is correct.
	// In particular, I think job.populateServicesIngresses is wrong.
	// Hence, we return an error until we can make sure that the code is correct.
	// The next error is redundant with this one, but we leave both since we may wish to remove this one.
	// - Albin
	if len(j.PodSpecs) > 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpecs",
			Value:   j.PodSpecs,
			Message: "Jobs with multiple pods are not supported",
		})
	}

	// I'm not convinced the code is correct when combining j.PodSpec and j.PodSpecs.
	// We should do more testing to make sure it's safe before we allow it.
	// - Albin
	if len(j.PodSpecs) > 0 && j.PodSpec != nil {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   j.PodSpec,
			Message: "PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)",
		})
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
			return errors.Errorf("ingress contains zero ports. Each ingress should have at least one port.")
		}

		for _, port := range portConfig.Ports {
			if existingIndex, existing := existingPortSet[port]; existing {
				return errors.Errorf(
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
