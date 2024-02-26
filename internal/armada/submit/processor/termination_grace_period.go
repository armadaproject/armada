package processor

import (
	"time"

	v1 "k8s.io/api/core/v1"
)

type terminationGracePeriodProcessor struct {
	podSpecProcessor
	minTerminationGracePeriod time.Duration
}

func (p terminationGracePeriodProcessor) processPodSpec(spec *v1.PodSpec) {
	if p.minTerminationGracePeriod.Seconds() == 0 {
		return
	}
	var podTerminationGracePeriodSeconds int64
	if spec.TerminationGracePeriodSeconds != nil {
		podTerminationGracePeriodSeconds = *spec.TerminationGracePeriodSeconds
	}
	if podTerminationGracePeriodSeconds == 0 {
		defaultTerminationGracePeriodSeconds := int64(
			p.minTerminationGracePeriod.Seconds(),
		)
		spec.TerminationGracePeriodSeconds = &defaultTerminationGracePeriodSeconds
	}
}
