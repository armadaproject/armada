package server

import (
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
)

func applyDefaultsToAnnotations(annotations map[string]string, config configuration.SubmissionConfig) {
	if annotations == nil {
		return
	}
	applyDefaultNodeUniformityLabelAnnotation(annotations, config)
}

func applyDefaultNodeUniformityLabelAnnotation(annotations map[string]string, config configuration.SubmissionConfig) {
	if _, ok := annotations[configuration.GangIdAnnotation]; ok {
		if _, ok := annotations[configuration.GangNodeUniformityLabelAnnotation]; !ok {
			annotations[configuration.GangNodeUniformityLabelAnnotation] = config.DefaultGangNodeUniformityLabel
		}
	}
}

func applyDefaultsToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if spec == nil {
		return
	}
	applyDefaultPriorityClassNameToPodSpec(spec, config)
	applyDefaultRequestsAndLimitsToPodSpec(spec, config)
	applyDefaultTolerationsToPodSpec(spec, config)
	applyDefaultActiveDeadlineSecondsToPodSpec(spec, config)
	applyDefaultTerminationGracePeriodToPodSpec(spec, config)
}

func applyDefaultRequestsAndLimitsToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	for i := range spec.Containers {
		c := &spec.Containers[i]
		if c.Resources.Limits == nil {
			c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
		}
		if c.Resources.Requests == nil {
			c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
		}
		for res, val := range config.DefaultJobLimits {
			_, hasLimit := c.Resources.Limits[v1.ResourceName(res)]
			_, hasRequest := c.Resources.Limits[v1.ResourceName(res)]
			if !hasLimit && !hasRequest {
				c.Resources.Requests[v1.ResourceName(res)] = val
				c.Resources.Limits[v1.ResourceName(res)] = val
			}
		}
	}
}

func applyDefaultTolerationsToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	spec.Tolerations = append(spec.Tolerations, config.DefaultJobTolerations...)
	if config.DefaultJobTolerationsByPriorityClass != nil {
		if tolerations, ok := config.DefaultJobTolerationsByPriorityClass[spec.PriorityClassName]; ok {
			spec.Tolerations = append(spec.Tolerations, tolerations...)
		}
	}
	if config.DefaultJobTolerationsByResourceRequest != nil {
		resourceRequest := armadaresource.TotalPodResourceRequest(spec)
		for resourceType, value := range resourceRequest {
			if value.Cmp(resource.Quantity{}) <= 0 {
				// Skip for resource specified but 0
				continue
			}
			if tolerations, ok := config.DefaultJobTolerationsByResourceRequest[resourceType]; ok {
				spec.Tolerations = append(spec.Tolerations, tolerations...)
			}
		}
	}
}

func applyDefaultPriorityClassNameToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if spec.PriorityClassName == "" {
		spec.PriorityClassName = config.DefaultPriorityClassName
	}
}

// applyDefaultTerminationGracePeriodToPodSpec sets the termination grace period
// of the pod equal to the minimum if
// - the pod does not explicitly set a termination period, or
// - the pod explicitly sets a termination period of 0.
func applyDefaultTerminationGracePeriodToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if config.MinTerminationGracePeriod.Seconds() == 0 {
		return
	}
	var podTerminationGracePeriodSeconds int64
	if spec.TerminationGracePeriodSeconds != nil {
		podTerminationGracePeriodSeconds = *spec.TerminationGracePeriodSeconds
	}
	if podTerminationGracePeriodSeconds == 0 {
		defaultTerminationGracePeriodSeconds := int64(
			config.MinTerminationGracePeriod.Seconds(),
		)
		spec.TerminationGracePeriodSeconds = &defaultTerminationGracePeriodSeconds
	}
}

func applyDefaultActiveDeadlineSecondsToPodSpec(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if spec.ActiveDeadlineSeconds != nil {
		return
	}
	var activeDeadlineSeconds float64
	for resourceType, activeDeadlineForResource := range config.DefaultActiveDeadlineByResourceRequest {
		for _, c := range spec.Containers {
			q := c.Resources.Requests[v1.ResourceName(resourceType)]
			if q.Cmp(resource.Quantity{}) == 1 && activeDeadlineForResource.Seconds() > activeDeadlineSeconds {
				activeDeadlineSeconds = activeDeadlineForResource.Seconds()
			}
		}
	}
	if activeDeadlineSeconds == 0 {
		activeDeadlineSeconds = config.DefaultActiveDeadline.Seconds()
	}
	if activeDeadlineSeconds != 0 {
		v := int64(math.Ceil(activeDeadlineSeconds))
		spec.ActiveDeadlineSeconds = &v
	}
}

// fillContainerRequestsAndLimits updates resource's requests/limits of container to match the value of
// limits/requests if the resource doesn't have requests/limits setup. If a Container specifies its own
// memory limit, but does not specify a memory request, assign a memory request that matches the limit.
// Similarly, if a Container specifies its own CPU limit, but does not specify a CPU request, automatically
// assigns a CPU request that matches the limit.
//
// 2024-03-18 chrisma: This seems suboptimal. return a string we can log out if people are submitting sparse requests.
// If nobody is using this then remove this.
func fillContainerRequestsAndLimits(containers []v1.Container) string {
	infoMsg := ""
	for index := range containers {
		if containers[index].Resources.Limits == nil {
			containers[index].Resources.Limits = v1.ResourceList{}
		}
		if containers[index].Resources.Requests == nil {
			containers[index].Resources.Requests = v1.ResourceList{}
		}

		for resourceName, quantity := range containers[index].Resources.Limits {
			if _, ok := containers[index].Resources.Requests[resourceName]; !ok {
				containers[index].Resources.Requests[resourceName] = quantity
				if infoMsg == "" {
					infoMsg = fmt.Sprintf("container %s had limits but not requests for %s", containers[index].Name, resourceName)
				}
			}
		}
		for resourceName, quantity := range containers[index].Resources.Requests {
			if _, ok := containers[index].Resources.Limits[resourceName]; !ok {
				containers[index].Resources.Limits[resourceName] = quantity
				if infoMsg == "" {
					infoMsg = fmt.Sprintf("container %s had requests but not limits for %s", containers[index].Name, resourceName)
				}
			}
		}
	}
	return infoMsg
}
