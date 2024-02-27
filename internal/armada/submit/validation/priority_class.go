package validation

import (
	"fmt"
	"github.com/armadaproject/armada/internal/common/types"
	v1 "k8s.io/api/core/v1"
)

type priorityClassValidator struct {
	podSpecValidator
	allowedPriorityClasses map[string]types.PriorityClass
}

func (p priorityClassValidator) validatePodSpec(spec *v1.PodSpec) error {
	priorityClassName := spec.PriorityClassName
	if _, exists := p.allowedPriorityClasses[priorityClassName]; !exists {
		return fmt.Errorf("priority class %s is not supported", priorityClassName)
	}
	return nil
}
