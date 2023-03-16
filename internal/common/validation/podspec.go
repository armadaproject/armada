package validation

import (
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func ValidatePodSpec(spec *v1.PodSpec, schedulingConfig *configuration.SchedulingConfig) error {
	if spec == nil {
		return errors.Errorf("empty pod spec")
	}

	if uint(spec.Size()) > schedulingConfig.MaxPodSpecSizeBytes {
		return errors.Errorf("pod spec has a size of %v bytes which is greater than the maximum allowed size of %v", spec.Size(), schedulingConfig.MaxPodSpecSizeBytes)
	}

	if len(spec.Containers) == 0 {
		return errors.Errorf("pod spec has no containers")
	}

	err := validateAffinity(spec.Affinity)
	if err != nil {
		return err
	}

	err = validateTerminationGracePeriod(spec, schedulingConfig)
	if err != nil {
		return err
	}

	for _, container := range spec.Containers {
		if len(container.Resources.Limits) == 0 {
			return errors.Errorf("container %v has no resource limits specified", container.Name)
		}
		if len(container.Resources.Requests) == 0 {
			return errors.Errorf("container %v has no resource requests specified", container.Name)
		}
		err = validateContainerResource(container.Resources.Limits, schedulingConfig.MinJobResources, container.Name, "limit")
		if err != nil {
			return err
		}
		err = validateContainerResource(container.Resources.Requests, schedulingConfig.MinJobResources, container.Name, "request")
		if err != nil {
			return err
		}
		if !resourceListEquals(container.Resources.Requests, container.Resources.Limits) {
			return errors.Errorf("container %v does not have resource request and limit equal (this is currently not supported)", container.Name)
		}
	}
	return validatePorts(spec)
}

func validateTerminationGracePeriod(spec *v1.PodSpec, config *configuration.SchedulingConfig) error {
	specHasTerminationGracePeriod := spec.TerminationGracePeriodSeconds != nil
	var terminationGracePeriodSeconds int64
	var exceedsBounds bool
	if specHasTerminationGracePeriod {
		terminationGracePeriodSeconds = *spec.TerminationGracePeriodSeconds
		exceedsBounds = (terminationGracePeriodSeconds < int64(config.MinTerminationGracePeriod.Seconds()) ||
			terminationGracePeriodSeconds > int64(config.MaxTerminationGracePeriod.Seconds()))
	}
	if exceedsBounds {
		return errors.Errorf("terminationGracePeriodSeconds of %v must be in [%d, %d]s, or omitted",
			terminationGracePeriodSeconds,
			int64(config.MinTerminationGracePeriod.Seconds()),
			int64(config.MaxTerminationGracePeriod.Seconds()),
		)
	}
	return nil
}

func validateContainerResource(
	resourceSpec v1.ResourceList,
	minJobResources v1.ResourceList,
	containerName string,
	requestType string,
) error {
	for rc, containerRsc := range resourceSpec {
		serverRsc, nonEmpty := minJobResources[rc]
		if nonEmpty && containerRsc.Value() < serverRsc.Value() {
			return errors.Errorf(
				"[validateContainerResource] container %q %s %s (%s) below server minimum (%s)",
				containerName,
				rc,
				requestType,
				&containerRsc,
				&serverRsc,
			)
		}
	}
	return nil
}

func validateAffinity(affinity *v1.Affinity) error {
	if affinity == nil {
		return nil
	}

	nodeAffinity := affinity.NodeAffinity
	if nodeAffinity == nil {
		return nil
	}

	err := validatePreferredNodeAffinityNotPresent(nodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	if err != nil {
		return err
	}

	return validateRequiredNodeAffinity(nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
}

func validatePreferredNodeAffinityNotPresent(preferred []v1.PreferredSchedulingTerm) error {
	if preferred != nil && len(preferred) > 0 {
		return errors.New("PreferredDuringSchedulingIgnoredDuringExecution node affinity is not supported by Armada")
	}
	return nil
}

func validateRequiredNodeAffinity(required *v1.NodeSelector) error {
	if required == nil {
		return nil
	}

	_, err := nodeaffinity.NewNodeSelector(required)
	if err != nil {
		return errors.Errorf("invalid RequiredDuringSchedulingIgnoredDuringExecution node affinity: %v", err)
	}
	return nil
}

func resourceListEquals(a v1.ResourceList, b v1.ResourceList) bool {
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

func validatePorts(podSpec *v1.PodSpec) error {
	existingPortSet := make(map[int32]int)
	for index, container := range podSpec.Containers {
		for _, port := range container.Ports {
			if existingIndex, existing := existingPortSet[port.ContainerPort]; existing {
				return errors.Errorf(
					"container port %d is exposed multiple times, specified in containers with indexes %d, %d. Should only be exposed once",
					port.ContainerPort, existingIndex, index)
			} else {
				existingPortSet[port.ContainerPort] = index
			}
		}
	}
	return nil
}

func validatePodSpecPriorityClass(podSpec *v1.PodSpec, preemptionEnabled bool, allowedPriorityClasses map[string]configuration.PriorityClass) error {
	priorityClassName := podSpec.PriorityClassName
	if priorityClassName != "" {
		if !preemptionEnabled {
			return errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PriorityClassName",
				Value:   podSpec.PriorityClassName,
				Message: "Preemption is disabled in Server config",
			})
		}
		if _, exists := allowedPriorityClasses[priorityClassName]; !exists {
			return errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "PriorityClassName",
				Value:   podSpec.PriorityClassName,
				Message: "Specified Priority Class is not supported in Server config",
			})
		}
	}

	return nil
}
