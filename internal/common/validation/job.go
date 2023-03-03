package validation

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/scheduler"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/api"
)

func ValidateApiJobs(jobs []*api.Job, config configuration.SchedulingConfig) error {
	err := validateGangs(jobs, configuration.GangIdAnnotation, configuration.GangCardinalityAnnotation)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if err := ValidateApiJob(job, config); err != nil {
			return err
		}
	}
	return nil
}

func validateGangs(jobs []*api.Job, gangIdAnnotation, gangCardinalityAnnotation string) error {
	gangDetailsByGangId := make(map[string]struct {
		actualCardinality         int
		expectedCardinality       int
		expectedPriorityClassName string
	})
	for i, job := range jobs {
		annotations := job.Annotations
		gangId, gangCardinality, isGangJob, err := scheduler.GangIdAndCardinalityFromAnnotations(annotations, gangIdAnnotation, gangCardinalityAnnotation)
		if err != nil {
			return errors.WithMessagef(err, "%d-th job with id %s in gang %s", i, job.Id, gangId)
		}
		if !isGangJob {
			continue
		}
		if gangId == "" {
			return errors.Errorf("empty gang id for %d-th job with id %s", i, job.Id)
		}
		podSpec := util.PodSpecFromJob(job)
		if details, ok := gangDetailsByGangId[gangId]; ok {
			if details.expectedCardinality != gangCardinality {
				return errors.Errorf(
					"inconsistent gang cardinality for %d-th job with id %s in gang %s: expected %d but got %d",
					i, job.Id, gangId, details.expectedCardinality, gangCardinality,
				)
			}
			if podSpec != nil && details.expectedPriorityClassName != podSpec.PriorityClassName {
				return errors.Errorf(
					"inconsistent PriorityClassName for %d-th job with id %s in gang %s: expected %s but got %s",
					i, job.Id, gangId, details.expectedPriorityClassName, podSpec.PriorityClassName,
				)
			}
			details.actualCardinality++
			gangDetailsByGangId[gangId] = details
		} else {
			details.actualCardinality = 1
			details.expectedCardinality = gangCardinality
			if podSpec != nil {
				details.expectedPriorityClassName = podSpec.PriorityClassName
			}
			gangDetailsByGangId[gangId] = details
		}
	}
	for gangId, details := range gangDetailsByGangId {
		if details.expectedCardinality != details.actualCardinality {
			return errors.Errorf(
				"unexpected number of jobs for gang %s: expected %d jobs but got %d",
				gangId, details.expectedCardinality, details.actualCardinality,
			)
		}
	}
	return nil
}

func ValidateApiJob(job *api.Job, config configuration.SchedulingConfig) error {
	if err := ValidateApiJobPodSpecs(job); err != nil {
		return err
	}
	if config.Preemption.Enabled {
		if err := validatePodSpecPriorityClass(job.PodSpec, config.Preemption.Enabled, config.Preemption.PriorityClasses); err != nil {
			return err
		}
		for _, podSpec := range job.PodSpecs {
			if err := validatePodSpecPriorityClass(podSpec, config.Preemption.Enabled, config.Preemption.PriorityClasses); err != nil {
				return err
			}
		}
	}

	return nil
}

func ValidateApiJobPodSpecs(j *api.Job) error {
	if j.PodSpec == nil && len(j.PodSpecs) == 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   j.PodSpec,
			Message: "Job does not contain at least one PodSpec",
		})
	}

	// We only support jobs with a single PodSpec, and it must be set to j.PodSpec.
	if j.PodSpec == nil && len(j.PodSpecs) == 1 {
		j.PodSpec = j.PodSpecs[0]
		j.PodSpecs = nil
	}

	// I'm not convinced that the code to create services/ingresses when multiple pods are submitted is correct.
	// In particular, I think job.populateServicesIngresses is wrong.
	// Hence, we return an error until we can make sure that the code is correct.
	// The next error is redundant with this one, but we leave both since we may wish to remove this one.
	// - Albin
	if len(j.PodSpecs) > 0 {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpecs",
			Value:   j.PodSpecs,
			Message: "Jobs with multiple pods are not supported",
		})
	}

	// I'm not convinced the code is correct when combining j.PodSpec and j.PodSpecs.
	// We should do more testing to make sure it's safe before we allow it.
	// - Albin
	if len(j.PodSpecs) > 0 && j.PodSpec != nil {
		return errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PodSpec",
			Value:   j.PodSpec,
			Message: "PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)",
		})
	}

	return nil
}

func ValidateJobSubmitRequestItem(request *api.JobSubmitRequestItem) error {
	return validateIngressConfigs(request)
}

func validateIngressConfigs(item *api.JobSubmitRequestItem) error {
	existingPortSet := make(map[uint32]int)

	for index, portConfig := range item.Ingress {
		if len(portConfig.Ports) == 0 {
			return errors.Errorf("ingress contains zero ports. Each ingress should have at least one port.")
		}

		for _, port := range portConfig.Ports {
			if existingIndex, existing := existingPortSet[port]; existing {
				return errors.Errorf(
					"port %d has two ingress configurations, specified in ingress configs with indexes %d, %d. Each port should at maximum have one ingress configuration",
					port,
					existingIndex,
					index,
				)
			} else {
				existingPortSet[port] = index
			}
		}
	}
	return nil
}
