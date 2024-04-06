package defaults

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type Processor interface {
	Apply(msg *armadaevents.SubmitJob)
}

func ApplyDefaults(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig) {
	processors := []Processor{
		activeDeadlineSecondsProcessor{
			defaultActiveDeadline:                  config.DefaultActiveDeadline,
			defaultActiveDeadlineByResourceRequest: config.DefaultActiveDeadlineByResourceRequest,
		},
		gangAnnotationProcessor{defaultGangNodeUniformityLabel: config.DefaultGangNodeUniformityLabel},
		priorityClassProcessor{defaultPriorityClass: config.DefaultPriorityClassName},
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

func processPodSpec(msg *armadaevents.SubmitJob, f func(*v1.PodSpec)) {
	if msg.MainObject != nil {
		switch typed := msg.MainObject.Object.(type) {
		case *armadaevents.KubernetesMainObject_PodSpec:
			f(typed.PodSpec.PodSpec)
		}
	}
}
