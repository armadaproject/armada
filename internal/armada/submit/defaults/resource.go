package defaults

import (
	"github.com/armadaproject/armada/pkg/armadaevents"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

type resourceProcessor struct {
	defaultJobLimits armadaresource.ComputeResources
}

func (p resourceProcessor) Apply(msg *armadaevents.SubmitJob) {
	processPodSpec(msg, func(spec *v1.PodSpec) {
		for i := range spec.Containers {
			c := &spec.Containers[i]
			if c.Resources.Limits == nil {
				c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
			}
			if c.Resources.Requests == nil {
				c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
			}
			for res, val := range p.defaultJobLimits {
				_, hasLimit := c.Resources.Limits[v1.ResourceName(res)]
				_, hasRequest := c.Resources.Limits[v1.ResourceName(res)]
				if !hasLimit && !hasRequest {
					c.Resources.Requests[v1.ResourceName(res)] = val
					c.Resources.Limits[v1.ResourceName(res)] = val
				}
			}
		}
	})
}
