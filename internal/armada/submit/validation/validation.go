package validation

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/pkg/errors"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"github.com/armadaproject/armada/internal/armada/configuration"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/pkg/api"
)

// requestValidator is a function that validates the entire JobSubmitRequest
type requestValidator func(msg *api.JobSubmitRequest, config configuration.SubmissionConfig) error

// itemValidator is a function that validates an individual JobSubmitRequestItem
type itemValidator func(spec *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error

var (
	requestValidators = []requestValidator{
		validateHasQueue,
		validateGangs,
	}
	itemValidators = []itemValidator{
		validateHasNamespace,
		validateHasPodSpec,
		validatePodSpecSize,
		validateAffinity,
		validateResources,
		validatePriorityClasses,
		validateTerminationGracePeriod,
		validateIngresses,
		validatePorts,
	}
)

func ValidateSubmitRequest(req *api.JobSubmitRequest, config configuration.SubmissionConfig) error {
	for _, validationFunc := range requestValidators {
		err := validationFunc(req, config)
		if err != nil {
			return err
		}
	}

	for _, item := range req.JobRequestItems {
		for _, validationFunc := range itemValidators {
			err := validationFunc(item, config)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func validatePodSpecSize(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()

	if uint(spec.Size()) > config.MaxPodSpecSizeBytes {
		return errors.Errorf(
			"Pod spec has a size of %d bytes which is greater than the maximum allowed size of %d",
			spec.Size(),
			config.MaxPodSpecSizeBytes)
	}
	return nil
}

func validateIngresses(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	existingPortSet := make(map[uint32]int)

	for index, portConfig := range j.Ingress {
		if len(portConfig.Ports) == 0 {
			return fmt.Errorf("ingress contains zero ports. Each ingress should have at least one port")
		}

		for _, port := range portConfig.Ports {
			if existingIndex, existing := existingPortSet[port]; existing {
				return fmt.Errorf(
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

func validateHasNamespace(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	if len(j.Namespace) == 0 {
		return fmt.Errorf("namespace is a required field")
	}
	return nil
}

func validateHasPodSpec(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	if j.PodSpec == nil && len(j.PodSpecs) == 0 {
		return errors.Errorf("Job must contain at least one PodSpec")
	}

	if len(j.PodSpecs) > 0 && j.PodSpec != nil {
		return errors.Errorf("PodSpec must be nil if PodSpecs is provided (i.e., these are exclusive)")
	}

	if len(j.PodSpecs) > 1 {
		return errors.Errorf("Jobs with multiple pods are not supported")
	}

	return nil
}

func validateHasQueue(r *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	if len(r.Queue) == 0 {
		return fmt.Errorf("queue is a required field")
	}
	return nil
}

func validatePorts(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	existingPortSet := make(map[int32]int)
	for index, container := range spec.Containers {
		for _, port := range container.Ports {
			if existingIndex, existing := existingPortSet[port.ContainerPort]; existing {
				return fmt.Errorf(
					"container port %d is exposed multiple times, specified in containers with indexes %d, %d. Should only be exposed once",
					port.ContainerPort, existingIndex, index)
			} else {
				existingPortSet[port.ContainerPort] = index
			}
		}
	}
	return nil
}

func validateAffinity(j *api.JobSubmitRequestItem, _ configuration.SubmissionConfig) error {
	affinity := j.GetMainPodSpec().Affinity
	if affinity == nil {
		return nil // No affinity to check
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil // No affinity to check
	}

	// We don't support PreferredDuringSchedulingIgnoredDuringExecution
	if len(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution) > 0 {
		return fmt.Errorf("preferredDuringSchedulingIgnoredDuringExecution node affinity is not supported by Armada")
	}

	// Check that RequiredDuringSchedulingIgnoredDuringExecution is actually a valid affinity rule
	if nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		_, err := nodeaffinity.NewNodeSelector(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
		if err != nil {
			return fmt.Errorf("invalid RequiredDuringSchedulingIgnoredDuringExecution node affinity: %v", err)
		}
	}
	return nil
}

func validatePriorityClasses(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}

	priorityClassName := spec.PriorityClassName
	if priorityClassName == "" {
		return nil
	}

	if exists := config.AllowedPriorityClassNames[priorityClassName]; !exists {
		return fmt.Errorf("priority class %s is not supported", priorityClassName)
	}
	return nil
}

func validateResources(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	// Function which tells us if two k8s resource lists contain exactly the same elements
	resourceListEquals := func(a v1.ResourceList, b v1.ResourceList) bool {
		if len(a) != len(b) {
			return false
		}
		for k, v := range a {
			if v != b[k] {
				return false
			}
		}
		return true
	}

	spec := j.GetMainPodSpec()
	for _, container := range spec.Containers {

		if len(container.Resources.Requests) == 0 && len(container.Resources.Requests) == 0 {
			return fmt.Errorf("container %v has no resources specified", container.Name)
		}

		if !resourceListEquals(container.Resources.Requests, container.Resources.Limits) {
			return fmt.Errorf("container %v does not have resource request and limit equal (this is currently not supported)", container.Name)
		}

		for rc, containerRsc := range container.Resources.Requests {
			serverRsc, nonEmpty := config.MinJobResources[rc]
			if nonEmpty && containerRsc.Value() < serverRsc.Value() {
				return fmt.Errorf(
					"container %q %s requests (%s) below server minimum (%s)",
					container.Name,
					rc,
					&containerRsc,
					&serverRsc,
				)
			}
		}
	}
	return nil
}

// jobAdapter turns JobSubmitRequestItem into a Minimal Job
type jobAdapter struct {
	*api.JobSubmitRequestItem
}

func (j jobAdapter) GetPriorityClassName() string {
	if j.PodSpec != nil {
		return j.PodSpec.PriorityClassName
	} else if len(j.PodSpecs) > 0 {
		return j.PodSpecs[0].PriorityClassName
	}
	return ""
}

func validateGangs(request *api.JobSubmitRequest, _ configuration.SubmissionConfig) error {
	gangDetailsByGangId := make(map[string]schedulercontext.GangInfo)
	for _, job := range request.JobRequestItems {
		actual, err := schedulercontext.GangInfoFromLegacySchedulerJob(jobAdapter{job})
		if err != nil {
			return fmt.Errorf("invalid gang annotations: %s", err.Error())
		}
		if actual.Id == "" {
			continue
		}
		if expected, ok := gangDetailsByGangId[actual.Id]; ok {
			if expected.Cardinality != actual.Cardinality {
				return errors.Errorf(
					"inconsistent gang cardinality in gang %s: expected %d but got %d",
					actual.Id, expected.Cardinality, actual.Cardinality,
				)
			}
			if expected.MinimumCardinality != actual.MinimumCardinality {
				return errors.Errorf(
					"inconsistent gang minimum cardinality in gang %s: expected %d but got %d",
					actual.Id, expected.MinimumCardinality, actual.MinimumCardinality,
				)
			}
			if expected.PriorityClassName != actual.PriorityClassName {
				return errors.Errorf(
					"inconsistent PriorityClassName in gang %s: expected %s but got %s",
					actual.Id, expected.PriorityClassName, actual.PriorityClassName,
				)
			}
			if actual.NodeUniformity != expected.NodeUniformity {
				return errors.Errorf(
					"inconsistent nodeUniformityLabel in gang %s: expected %s but got %s",
					actual.Id, expected.NodeUniformity, actual.NodeUniformity,
				)
			}
		} else {
			gangDetailsByGangId[actual.Id] = actual
		}
	}
	return nil
}

func validateTerminationGracePeriod(j *api.JobSubmitRequestItem, config configuration.SubmissionConfig) error {
	spec := j.GetMainPodSpec()
	if spec == nil {
		return nil
	}
	if spec.TerminationGracePeriodSeconds != nil {
		terminationGracePeriodSeconds := *spec.TerminationGracePeriodSeconds
		if terminationGracePeriodSeconds < int64(config.MinTerminationGracePeriod.Seconds()) ||
			terminationGracePeriodSeconds > int64(config.MaxTerminationGracePeriod.Seconds()) {
			return fmt.Errorf(
				"terminationGracePeriodSeconds of %d must be [%d, %d], or omitted",
				terminationGracePeriodSeconds,
				config.MinTerminationGracePeriod,
				config.MaxTerminationGracePeriod)
		}
	}
	return nil
}
