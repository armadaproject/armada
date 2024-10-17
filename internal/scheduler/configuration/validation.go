package configuration

import (
	"fmt"

	"github.com/go-playground/validator/v10"
)

func (c Configuration) Validate() error {
	validate := validator.New()
	validate.RegisterStructValidation(SchedulingConfigValidation, SchedulingConfig{})
	return validate.Struct(c)
}

func SchedulingConfigValidation(sl validator.StructLevel) {
	c := sl.Current().Interface().(SchedulingConfig)

	wellKnownNodeTypes := make(map[string]bool)
	for i, wellKnownNodeType := range c.WellKnownNodeTypes {
		if wellKnownNodeTypes[wellKnownNodeType.Name] {
			fieldName := fmt.Sprintf("WellKnownNodeTypes[%d].Name", i)
			sl.ReportError(wellKnownNodeType.Name, fieldName, "", DuplicateWellKnownNodeTypeErrorMessage, "")
		}
		wellKnownNodeTypes[wellKnownNodeType.Name] = true
	}

	for priorityClassName, priorityClass := range c.PriorityClasses {
		if len(priorityClass.AwayNodeTypes) > 0 && !priorityClass.Preemptible {
			fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].Preemptible", priorityClassName)
			sl.ReportError(priorityClass.Preemptible, fieldName, "", AwayNodeTypesWithoutPreemptionErrorMessage, "")
		}

		for i, awayNodeType := range priorityClass.AwayNodeTypes {
			if !wellKnownNodeTypes[awayNodeType.WellKnownNodeTypeName] {
				fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypeName", priorityClassName, i)
				sl.ReportError(awayNodeType.WellKnownNodeTypeName, fieldName, "", UnknownWellKnownNodeTypeErrorMessage, "")
			}
		}
	}
}
