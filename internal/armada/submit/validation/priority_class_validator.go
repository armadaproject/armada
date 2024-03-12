package validation

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/types"
	"github.com/armadaproject/armada/pkg/api"
)

type priorityClassValidator struct {
	allowedPriorityClasses map[string]types.PriorityClass
}

func (p priorityClassValidator) Validate(j *api.JobSubmitRequestItem) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}

	priorityClassName := spec.PriorityClassName
	if priorityClassName == "" {
		return nil
	}

	if _, exists := p.allowedPriorityClasses[priorityClassName]; !exists {
		return fmt.Errorf("priority class %s is not supported", priorityClassName)
	}
	return nil
}
