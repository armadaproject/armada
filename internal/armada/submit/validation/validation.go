package validation

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/pkg/api"
)

func ValidateSubmitRequest(req *api.JobSubmitRequest, config configuration.SchedulingConfig) error {
	requestValidator := validation.NewCompoundValidator[*api.JobSubmitRequest](
		queueValidator{},
		gangValidator{})

	itemValidator := validation.NewCompoundValidator[*api.JobSubmitRequestItem](
		hasNamespaceValidator{},
		hasPodSpecValidator{},
		podSpecSizeValidator{},
		affinityValidator{},
		containerValidator{minJobResources: config.MinJobResources},
		priorityClassValidator{allowedPriorityClasses: config.Preemption.PriorityClasses},
		terminationGracePeriodValidator{
			minGracePeriod: int64(config.MinTerminationGracePeriod.Seconds()),
			maxGracePeriod: int64(config.MaxTerminationGracePeriod.Seconds()),
		},
		ingressValidator{},
		portsValidator{})

	// First apply a validators that need access to the entire job request
	if err := requestValidator.Validate(req); err != nil {
		return err
	}

	// Next apply validators that act on individual job items
	for _, reqItem := range req.JobRequestItems {
		if err := itemValidator.Validate(reqItem); err != nil {
			return err
		}
	}

	return nil
}
