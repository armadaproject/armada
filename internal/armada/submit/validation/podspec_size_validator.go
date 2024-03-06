package validation

import (
	"github.com/armadaproject/armada/pkg/api"
	"github.com/pkg/errors"
)

type podSpecSizeValidator struct {
	maxSize uint
}

func (p podSpecSizeValidator) Validate(j *api.JobSubmitRequestItem) error {

	spec := j.GetMainPodSpec()

	if spec == nil {
		return nil
	}

	if uint(spec.Size()) > p.maxSize {
		return errors.Errorf(
			"Pod spec has a size of %d bytes which is greater than the maximum allowed size of %d",
			spec.Size(),
			p.maxSize)
	}
	return nil
}
