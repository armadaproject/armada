package conversion

import (
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type msgLevelProcessor func(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig)
type podLevelProcessor func(spec *v1.PodSpec, config configuration.SubmissionConfig)

var (
	msgLevelProcessors = []msgLevelProcessor{
		templateMeta,
		defaultGangNodeUniformityLabel,
	}
	podLevelProcessors = []podLevelProcessor{
		defaultActiveDeadlineSeconds,
		defaultPriorityClass,
		defaultResource,
		defaultTolerations,
		defaultTerminationGracePeriod,
	}
)

func postProcess(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig) {
	for _, p := range msgLevelProcessors {
		p(msg, config)
	}

	podSpec := msg.GetMainObject().GetPodSpec().GetPodSpec()
	if podSpec != nil {
		for _, p := range podLevelProcessors {
			p(podSpec, config)
		}
	}
}

func defaultActiveDeadlineSeconds(spec *v1.PodSpec, config configuration.SubmissionConfig) {
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

func defaultPriorityClass(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if spec.PriorityClassName == "" {
		spec.PriorityClassName = config.DefaultPriorityClassName
	}
}

func defaultResource(spec *v1.PodSpec, config configuration.SubmissionConfig) {
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

func defaultTolerations(spec *v1.PodSpec, config configuration.SubmissionConfig) {
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

func defaultTerminationGracePeriod(spec *v1.PodSpec, config configuration.SubmissionConfig) {
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

func defaultGangNodeUniformityLabel(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig) {
	annotations := msg.MainObject.GetObjectMeta().GetAnnotations()
	if annotations == nil {
		return
	}
	if _, ok := annotations[configuration.GangIdAnnotation]; ok {
		if _, ok := annotations[configuration.GangNodeUniformityLabelAnnotation]; !ok {
			annotations[configuration.GangNodeUniformityLabelAnnotation] = config.DefaultGangNodeUniformityLabel
		}
	}
}

func templateMeta(msg *armadaevents.SubmitJob, _ configuration.SubmissionConfig) {
	template := func(labels map[string]string, jobId string) {
		for key, value := range labels {
			value := strings.ReplaceAll(value, "{{JobId}}", ` \z`) // \z cannot be entered manually, hence its use
			value = strings.ReplaceAll(value, "{JobId}", jobId)
			labels[key] = strings.ReplaceAll(value, ` \z`, "JobId")
		}
	}

	jobId := armadaevents.MustUlidStringFromProtoUuid(msg.JobId)
	template(msg.MainObject.GetObjectMeta().GetAnnotations(), jobId)
	template(msg.MainObject.GetObjectMeta().GetLabels(), jobId)
}
