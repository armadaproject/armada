package validation

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

type terminationGracePeriodValidator struct {
	minGracePeriod int64
	maxGracePeriod int64
}

func (p terminationGracePeriodValidator) Validate(j *api.JobSubmitRequestItem) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}
	if spec.TerminationGracePeriodSeconds != nil {
		terminationGracePeriodSeconds := *spec.TerminationGracePeriodSeconds
		if terminationGracePeriodSeconds < p.minGracePeriod ||
			terminationGracePeriodSeconds > p.maxGracePeriod {
			return fmt.Errorf(
				"terminationGracePeriodSeconds of %d must be [%d, %d], or omitted",
				terminationGracePeriodSeconds,
				p.minGracePeriod,
				p.maxGracePeriod)
		}
	}
	return nil
}
