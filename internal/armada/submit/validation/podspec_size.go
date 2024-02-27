package validation

import (
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
)

type podSpecSizeValidator struct {
	podSpecValidator
	maxSize uint
}

func (p podSpecSizeValidator) validatePodSpec(spec *v1.PodSpec) error {
	if uint(spec.Size()) > p.maxSize {
		return errors.Errorf(
			"Pod spec has a size of %d bytes which is greater than the maximum allowed size of %d",
			spec.Size(),
			p.maxSize)
	}
	return nil
}
