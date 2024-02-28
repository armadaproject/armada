package processor

import (
	v1 "k8s.io/api/core/v1"
)

type priorityClassProcessor struct {
	podSpecProcessor
	defaultPriorityClass string
}

func (p priorityClassProcessor) processPodSpec(spec *v1.PodSpec) {
	if spec.PriorityClassName == "" {
		spec.PriorityClassName = p.defaultPriorityClass
	}
}
