package configuration

import (
	"fmt"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"

	"github.com/armadaproject/armada/internal/common/config"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/observability"
	"github.com/armadaproject/armada/internal/lookout/version"
)

func (c *Configuration) Mutate() (config.Config, error) {
	if c.MaxSchedulingDuration > 0 {
		log.Warnf("use of top level MaxSchedulingDuration has been deprecated - please use scheduling.MaxSchedulingDuration. Applying MaxSchedulingDuration to scheduling.MaxSchedulingDuration")
		c.Scheduling.MaxSchedulingDuration = c.MaxSchedulingDuration
	}

	if c.NewJobsSchedulingTimeout > 0 {
		log.Warnf("use of top level NewJobsSchedulingTimeout has been deprecated - please use scheduling.MaxNewJobSchedulingDuration. Applying NewJobsSchedulingTimeout to scheduling.MaxNewJobSchedulingDuration")
		c.Scheduling.MaxNewJobSchedulingDuration = c.NewJobsSchedulingTimeout
	}

	serviceInstance, err := os.Hostname()
	if err != nil {
		serviceInstance = uuid.New().String()
	}
	observabilityConfig, err := c.Observability.WithDefaults(observability.ResourceAttributes{
		ServiceName:     "scheduler",
		ServiceVersion:  version.Version,
		ServiceInstance: serviceInstance,
	})
	if err != nil {
		return nil, err
	}
	c.Observability = observabilityConfig

	return c, nil
}

func (c *Configuration) Validate() error {
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
			if awayNodeType.WellKnownNodeTypeName != "" && !wellKnownNodeTypes[awayNodeType.WellKnownNodeTypeName] {
				fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypeName", priorityClassName, i)
				sl.ReportError(awayNodeType.WellKnownNodeTypeName, fieldName, "", UnknownWellKnownNodeTypeErrorMessage, "")
			}

			for j, entry := range awayNodeType.NodeTypes {
				if !wellKnownNodeTypes[entry.Name] {
					fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].NodeTypes[%d].Name", priorityClassName, i, j)
					sl.ReportError(entry.Name, fieldName, "", UnknownWellKnownNodeTypeErrorMessage, "")
				}

				for k, cond := range entry.Conditions {
					validOps := map[string]bool{">": true, "<": true, "==": true}
					if !validOps[string(cond.Operator)] {
						fieldName := fmt.Sprintf("Preemption.PriorityClasses[%s].AwayNodeTypes[%d].WellKnownNodeTypes[%d].Conditions[%d].Operator", priorityClassName, i, j, k)
						sl.ReportError(cond.Operator, fieldName, "", InvalidAwayNodeTypeConditionOperatorErrorMessage, "")
					}
				}
			}
		}
	}
}
