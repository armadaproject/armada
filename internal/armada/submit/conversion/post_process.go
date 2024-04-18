package conversion

import (
	"math"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/armada/configuration"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type (
	// msgProcessor is function that modifies an armadaevents.SubmitJob in-place
	msgProcessor func(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig)
	// msgProcessor is function that modifies a v1.PodSpec in-place
	podProcessor func(spec *v1.PodSpec, config configuration.SubmissionConfig)
)

var (
	msgLevelProcessors = []msgProcessor{
		templateMeta,
		defaultGangNodeUniformityLabel,
	}
	podLevelProcessors = []podProcessor{
		defaultActiveDeadlineSeconds,
		defaultPriorityClass,
		defaultResource,
		defaultTolerations,
		defaultTerminationGracePeriod,
	}
)

// postProcess modifies an armadaevents.SubmitJob in-place by applying various rules to it.  This allows us
// to e.g. apply default values or template out the jobid.  The rules to be applied are defined above and may act at
// either the msg level or the podspec level
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

// defaultActiveDeadlineSeconds will modify  the ActiveDeadlineSeconds field on the podspec according to the following
// rules:
//   - If the podspec already  contains a non-nil ActiveDeadlineSeconds then no modification will occur.
//   - If the podpec's resources match a  per-resource default defined in config.DefaultActiveDeadlineByResourceRequest
//     then that value will be applied.  If multiple resources match, then the resource with the highest defaultActiveDeadline
//     will be chosen
//   - If no per-resource request defaults were found, then the podspec's ActiveDeadlineSeconds will be set to config.DefaultActiveDeadline.Seconds()
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

// Defaults the podspec's PriorityClassName to config.DefaultPriorityClassName if empty
func defaultPriorityClass(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	if spec.PriorityClassName == "" {
		spec.PriorityClassName = config.DefaultPriorityClassName
	}
}

// Adds resources defined in config.DefaultJobLimits to all containers in the podspec if that container is missing
// requests/limits for that particular resource. This can be used to e.g. ensure that lal jobs define at least some
// ephemeral storage.
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

// Adds tolerations to the podspec.  The tolerations added depend on three properties:
//   - config.DefaultJobTolerations: These tolerations are added to  all jobs
//   - config.DefaultJobTolerationsByPriorityClass: These tolerations are added to jobs based on the priority class of the
//     job. Typically, this allows preemptible jobs to run ina greater number of places.
//   - config.DefaultJobTolerationsByResourceRequest: These tolerations are added to jobs based on the resources they
//     request.  This is used to allow jobs requesting special hardware (e.g. gpu) to be able to run on machines containing
//     that hardware.
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

// Defaults the pod's TerminationGracePeriod if not filled in.
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

// Default's the job's GangNodeUniformityLabelAnnotation for gang jobs that do not define one.
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

// Templates the JobId in labels and annotations. This allows users to define labels and annotations containing the string
// {JobId} and have it populated with the actual id of the job.
func templateMeta(msg *armadaevents.SubmitJob, _ configuration.SubmissionConfig) {
	template := func(labels map[string]string, jobId string) {
		for key, value := range labels {
			value := strings.ReplaceAll(value, "{{JobId}}", ` \z`) // \z cannot be entered manually, hence its use
			value = strings.ReplaceAll(value, "{JobId}", jobId)
			labels[key] = strings.ReplaceAll(value, ` \z`, "JobId")
		}
	}

	jobId := armadaevents.MustUlidStringFromProtoUuid(msg.JobId)
	template(msg.GetObjectMeta().GetAnnotations(), jobId)
	template(msg.GetObjectMeta().GetLabels(), jobId)
}
