package validation

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

func ValidateSubmitRequest(req *api.JobSubmitRequest, config configuration.SchedulingConfig) error {

	requestValidator := validation.NewCompoundValidator[*api.JobSubmitRequest](
		queueValidator{},
		gangValidator{})

	itemValidator := validation.NewCompoundValidator[*api.JobSubmitRequestItem](
		namespaceValidator{},
		affinityValidator{},
		containerValidator{minJobResources: config.MinJobResources},
		ingressValidator{},
		numContainersValidator{},
		podSpecFieldValidator{},
		portsValidator{},
		terminationGracePeriodValidator{
			minTerminationGracePeriodSeconds: int64(config.MinTerminationGracePeriod),
			maxTerminationGracePeriodSeconds: int64(config.MaxTerminationGracePeriod),
		},
		priorityClassValidator{allowedPriorityClasses: config.Preemption.PriorityClasses})

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

type podSpecValidator struct{}

func (v podSpecValidator) Validate(j *api.JobSubmitRequestItem) error {
	podSpecs := []*v1.PodSpec{j.PodSpec}
	for _, spec := range podSpecs {
		if err := v.validatePodSpec(spec); err != nil {
			return err
		}
	}
	return nil
}

func (v podSpecValidator) validatePodSpec(_ *v1.PodSpec) error { return nil }
