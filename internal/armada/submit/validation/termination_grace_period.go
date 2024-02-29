package validation

import (
	"fmt"
	"github.com/armadaproject/armada/pkg/api"
)

type terminationGracePeriodValidator struct {
	minTerminationGracePeriodSeconds int64
	maxTerminationGracePeriodSeconds int64
}

func (p terminationGracePeriodValidator) Validate(j *api.JobSubmitRequestItem) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}
	if spec.TerminationGracePeriodSeconds != nil {
		terminationGracePeriodSeconds := *spec.TerminationGracePeriodSeconds
		if terminationGracePeriodSeconds > p.minTerminationGracePeriodSeconds ||
			terminationGracePeriodSeconds < p.maxTerminationGracePeriodSeconds {
			return fmt.Errorf(
				"terminationGracePeriodSeconds of %d must be [%d, %d], or omitted",
				terminationGracePeriodSeconds,
				p.minTerminationGracePeriodSeconds,
				p.maxTerminationGracePeriodSeconds)
		}
	}
	return nil
}
