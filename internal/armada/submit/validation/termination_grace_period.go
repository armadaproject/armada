package validation

import (
	"fmt"
	v1 "k8s.io/api/core/v1"
)

type terminationGracePeriodValidator struct {
	podSpecValidator
	minTerminationGracePeriodSeconds int64
	maxTerminationGracePeriodSeconds int64
}

func (p terminationGracePeriodValidator) validatePodSpec(spec *v1.PodSpec) error {
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
}
