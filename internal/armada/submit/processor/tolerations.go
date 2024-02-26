package processor

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

type tolerationsProcessor struct {
	podSpecProcessor
	defaultJobTolerations                  []v1.Toleration
	defaultJobTolerationsByPriorityClass   map[string][]v1.Toleration
	defaultJobTolerationsByResourceRequest map[string][]v1.Toleration
}

func (p tolerationsProcessor) processPodSpec(spec *v1.PodSpec) {
	spec.Tolerations = append(spec.Tolerations, p.defaultJobTolerations...)
	if p.defaultJobTolerationsByPriorityClass != nil {
		if tolerations, ok := p.defaultJobTolerationsByPriorityClass[spec.PriorityClassName]; ok {
			spec.Tolerations = append(spec.Tolerations, tolerations...)
		}
	}
	if p.defaultJobTolerationsByResourceRequest != nil {
		resourceRequest := armadaresource.TotalPodResourceRequest(spec)
		for resourceType, value := range resourceRequest {
			if value.Cmp(resource.Quantity{}) <= 0 {
				// Skip for resource specified but 0
				continue
			}
			if tolerations, ok := p.defaultJobTolerationsByResourceRequest[resourceType]; ok {
				spec.Tolerations = append(spec.Tolerations, tolerations...)
			}
		}
	}
}
