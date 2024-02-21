package job

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/pkg/api"
	v1 "k8s.io/api/core/v1"
)

type priorityClassValidator struct {
	allowedPriorityClasses map[string]types.PriorityClass
}

func (p priorityClassValidator) Validate(j *api.JobSubmitRequestItem) error {
	return validatePodSpecs(j, func(spec *v1.PodSpec) error {
		priorityClassName := spec.PriorityClassName
		if _, exists := p.allowedPriorityClasses[priorityClassName]; !exists {
			return fmt.Errorf("specified Priority Class is not supported in Server config")
		}
		return nil
	})
}
