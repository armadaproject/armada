package validation

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/validation"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

func NewJobValidator(config configuration.SchedulingConfig) validation.Validator[*api.JobSubmitRequestItem] {
	return validation.NewCompundValidator([]validation.Validator[*api.JobSubmitRequestItem]{
		namespaceValidator{},
		affinityValidator{},
		containerValidator{minJobResources: config.MinJobResources},
		ingressValidator{},
		numContainersValidator{},
		podSpecFieldValidator{},
		portsValidator{},
		priorityClassValidator{allowedPriorityClasses: config.Preemption.PriorityClasses},
		terminationGracePeriodValidator{
			minTerminationGracePeriodSeconds: int64(config.MinTerminationGracePeriod),
			maxTerminationGracePeriodSeconds: int64(config.MaxTerminationGracePeriod),
		},
	})
}

func NewJobRequestValidator() validation.Validator[*api.JobSubmitRequest] {
	return validation.NewCompundValidator([]validation.Validator[*api.JobSubmitRequest]{
		queueValidator{},
		gangValidator{},
	})
}

func validatePodSpecs(j *api.JobSubmitRequestItem, f func(spec *v1.PodSpec) error) error {
	podSpecs := []*v1.PodSpec{j.PodSpec}
	for _, spec := range podSpecs {
		if err := f(spec); err != nil {
			return err
		}
	}
	return nil
}
