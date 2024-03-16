package processor

import (
	"github.com/armadaproject/armada/pkg/armadaevents"

	v1 "k8s.io/api/core/v1"

	"time"
)

type terminationGracePeriodProcessor struct {
	minTerminationGracePeriod time.Duration
}

func (p terminationGracePeriodProcessor) Apply(msg *armadaevents.SubmitJob) {
	processPodSpec(msg, func(spec *v1.PodSpec) {
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
	})
}
