package validation

import (
	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/api"
)

type Validator[T any] interface {
	Validate(obj T) error
}

type CompoundValidator[T any] struct {
	validators []Validator[T]
}

func NewCompoundValidator[T any](validators ...Validator[T]) CompoundValidator[T] {
	return CompoundValidator[T]{
		validators: validators,
	}
}

func (c CompoundValidator[T]) Validate(obj T) error {
	for _, v := range c.validators {
		err := v.Validate(obj)
		if err != nil {
			return err
		}
	}
	return nil
}

func ValidateSubmitRequest(req *api.JobSubmitRequest, config configuration.SubmissionConfig) error {
	requestValidator := NewCompoundValidator[*api.JobSubmitRequest](
		queueValidator{},
		gangValidator{})

	itemValidator := NewCompoundValidator[*api.JobSubmitRequestItem](
		hasNamespaceValidator{},
		hasPodSpecValidator{},
		podSpecSizeValidator{maxSize: config.MaxPodSpecSizeBytes},
		affinityValidator{},
		resourcesValidator{minJobResources: config.MinJobResources},
		priorityClassValidator{allowedPriorityClasses: config.AllowedPriorityClassNames},
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
