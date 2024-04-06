package defaults

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type priorityClassProcessor struct {
	defaultPriorityClass string
}

func (p priorityClassProcessor) Apply(msg *armadaevents.SubmitJob) {
	processPodSpec(msg, func(spec *v1.PodSpec) {
		if spec.PriorityClassName == "" {
			spec.PriorityClassName = p.defaultPriorityClass
		}
	})
}
