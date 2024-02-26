package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
)

type podSpecFieldValidator struct{}

func (p podSpecFieldValidator) Validate(j *api.JobSubmitRequestItem) error {

	if j.PodSpec == nil && len(j.PodSpecs) == 0 {
		return errors.Errorf("Job does not contain at least one PodSpec")
	}

	if len(j.PodSpecs) > 0 && j.PodSpec != nil {
		return errors.Errorf("PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)")
	}

	// I'm not convinced that the code to create services/ingresses when multiple pods are submitted is correct.
	// In particular, I think job.populateServicesIngresses is wrong.
	// Hence, we return an error until we can make sure that the code is correct.
	// - Albin
	if len(j.PodSpecs) > 0 {
		return errors.Errorf("Jobs with multiple pods are not supported")
	}

	return nil
}
