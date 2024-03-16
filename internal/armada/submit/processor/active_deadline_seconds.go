package processor

import (
	"math"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type activeDeadlineSecondsProcessor struct {
	defaultActiveDeadline                  time.Duration
	defaultActiveDeadlineByResourceRequest map[string]time.Duration
}

func (p activeDeadlineSecondsProcessor) Apply(msg *armadaevents.SubmitJob) {
	processPodSpec(msg, func(spec *v1.PodSpec) {
		if spec.ActiveDeadlineSeconds != nil {
			return
		}
		var activeDeadlineSeconds float64
		for resourceType, activeDeadlineForResource := range p.defaultActiveDeadlineByResourceRequest {
			for _, c := range spec.Containers {
				q := c.Resources.Requests[v1.ResourceName(resourceType)]
				if q.Cmp(resource.Quantity{}) == 1 && activeDeadlineForResource.Seconds() > activeDeadlineSeconds {
					activeDeadlineSeconds = activeDeadlineForResource.Seconds()
				}
			}
		}
		if activeDeadlineSeconds == 0 {
			activeDeadlineSeconds = p.defaultActiveDeadline.Seconds()
		}
		if activeDeadlineSeconds != 0 {
			v := int64(math.Ceil(activeDeadlineSeconds))
			spec.ActiveDeadlineSeconds = &v
		}
	})
}
