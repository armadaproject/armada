package job

import (
	"fmt"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

type TerminationGracePeriodValidator struct {
	minTerminationGracePeriodSeconds int64
	maxTerminationGracePeriodSeconds int64
}

func (p *TerminationGracePeriodValidator) Validate(j *api.JobSubmitRequestItem) error {

	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		if spec.TerminationGracePeriodSeconds != nil {
			terminationGracePeriodSeconds := *spec.TerminationGracePeriodSeconds
			if terminationGracePeriodSeconds > p.minTerminationGracePeriodSeconds ||
				terminationGracePeriodSeconds < p.maxTerminationGracePeriodSeconds {
				return fmt.Errorf(
					"terminationGracePeriodSeconds of %d must be [%d, %d], or omitted",
					p.minTerminationGracePeriodSeconds,
					p.maxTerminationGracePeriodSeconds)
			}
		}
		return nil
	})

}
