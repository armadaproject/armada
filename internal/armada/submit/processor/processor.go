package processor

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type Processor interface {
	Apply(msg *armadaevents.SubmitJob)
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

func ApplyDefaults(msg *armadaevents.SubmitJob, config configuration.SchedulingConfig) {

	processors := []Processor{
		activeDeadlineSecondsProcessor{
			defaultActiveDeadline:                  config.DefaultActiveDeadline,
			defaultActiveDeadlineByResourceRequest: config.DefaultActiveDeadlineByResourceRequest,
		},
		gangAnnotationProcessor{defaultGangNodeUniformityLabel: config.DefaultGangNodeUniformityLabel},
		priorityClassProcessor{defaultPriorityClass: config.Preemption.DefaultPriorityClass},
		resourceProcessor{defaultJobLimits: nil},
		jobIdTemplateProcessor{},
		terminationGracePeriodProcessor{minTerminationGracePeriod: config.MinTerminationGracePeriod},
		tolerationsProcessor{
			defaultJobTolerations:                  config.DefaultJobTolerations,
			defaultJobTolerationsByPriorityClass:   config.DefaultJobTolerationsByPriorityClass,
			defaultJobTolerationsByResourceRequest: config.DefaultJobTolerationsByResourceRequest,
		},
	}

	for _, p := range processors {
		p.Apply(msg)
	}
}
