package job

import (
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

func validatePodSpecs(j *api.JobSubmitRequestItem, f func(spec *v1.PodSpec) error) error {
	podSpecs := []*v1.PodSpec{j.PodSpec}
	for _, spec := range podSpecs {
		if err := f(spec); err != nil {
			return err
		}
	}
	return nil
}
