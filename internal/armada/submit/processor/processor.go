package processor

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type Processor interface {
	Apply(msg *armadaevents.SubmitJob)
}

type CompoundProcessor struct {
	processors []Processor
}

func NewCompoundProcessor(processors ...Processor) CompoundProcessor {
	return CompoundProcessor{
		processors: processors,
	}
}

func (c CompoundProcessor) Apply(msg *armadaevents.SubmitJob) {
	for _, p := range c.processors {
		p.Apply(msg)
	}
}

type podSpecProcessor struct{}

func (p podSpecProcessor) Apply(msg *armadaevents.SubmitJob) {
	if msg.MainObject != nil {
		switch typed := msg.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			p.processPodSpec(typed.PodSpec.PodSpec)
		}
	}
}

func (p podSpecProcessor) processPodSpec(spec *v1.PodSpec) {}
