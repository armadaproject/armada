package job

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
)

type podSpecSizeValidator struct {
	maxSize uint
}

func (p podSpecSizeValidator) Validate(j *api.JobSubmitRequestItem) error {
	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		if uint(spec.Size()) > p.maxSize {
			return errors.Errorf(
				"Pod spec has a size of %d bytes which is greater than the maximum allowed size of %d",
				spec.Size(),
				p.maxSize)
		}
		return nil
	})
}
