package job

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

type PriorityClassValidator struct {
	PreemptionEnabled      bool
	AllowedPriorityClasses map[string]types.PriorityClass
}

func (p PriorityClassValidator) Validate(j *api.JobSubmitRequestItem) error {
	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		priorityClassName := spec.PriorityClassName
		if priorityClassName != "" && !p.PreemptionEnabled {
			return fmt.Errorf("preemption is disabled in Server config")
		}
		if _, exists := p.AllowedPriorityClasses[priorityClassName]; !exists {
			return fmt.Errorf("specified Priority Class is not supported in Server config")
		}
		return nil
	})
}
