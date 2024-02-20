package job

import (
	"fmt"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

type NumContainersValidator struct{}

func (p NumContainersValidator) Validate(j *api.JobSubmitRequestItem) error {
	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		if len(spec.Containers) == 0 {
			return fmt.Errorf("pod spec has no containers")
		}
		return nil
	})
}
