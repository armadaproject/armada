package configuration

import (
	"fmt"

	"github.com/go-playground/validator/v10"
)

func (c Configuration) Validate() error {
	// Validate scheduling timeout relationship
	if c.NewJobsSchedulingTimeout > 0 && c.NewJobsSchedulingTimeout >= c.MaxSchedulingDuration {
		return fmt.Errorf("%s: NewJobsSchedulingTimeout=%v, MaxSchedulingDuration=%v",
			InvalidSchedulingTimeoutErrorMessage,
			c.NewJobsSchedulingTimeout,
			c.MaxSchedulingDuration)
	}

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

		//for i, awayNodeType := range priorityClass.AwayNodeTypes {
		//for j, wkntConfig := range awayNodeType.WellKnownNodeTypes {
		//	if !wellKnownNodeTypes[wkntConfig.Name] {
		//		fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypes[%d].Name", priorityClassName, i, j)
		//		sl.ReportError(wkntConfig.Name, fieldName, "", UnknownWellKnownNodeTypeErrorMessage, "")
		//	}
		//	for k, cond := range wkntConfig.Conditions {
		//		validOps := map[string]bool{">": true, ">=": true, "<": true, "<=": true, "==": true}
		//		if !validOps[string(cond.Operator)] {
		//			fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypes[%d].Conditions[%d].Operator", priorityClassName, i, j, k)
		//			sl.ReportError(cond.Operator, fieldName, "", InvalidAwayNodeTypeConditionOperatorErrorMessage, "")
		//		}
		//		if _, err := k8sResource.ParseQuantity(cond.Value); err != nil {
		//			fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypes[%d].Conditions[%d].Value", priorityClassName, i, j, k)
		//			sl.ReportError(cond.Value, fieldName, "", InvalidAwayNodeTypeConditionValueErrorMessage, "")
		//		}
		//	}
		//}
		//}
	}
}
