package conversion

import (
	"math"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/constants"
	log "github.com/armadaproject/armada/internal/common/logging"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/server/configuration"
	"github.com/armadaproject/armada/pkg/api"
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
		normalizeGangInfo,
		templateMeta,
		defaultGangNodeUniformityLabel,
		defaultGangFailFastFlag,
		addGangIdLabel,
	}
	podLevelProcessors = []podProcessor{
		defaultActiveDeadlineSeconds,
		defaultPriorityClass,
		defaultResource,
		defaultTolerations,
		defaultTerminationGracePeriod,
	}
)

// submitJobAdapter turns SubmitJob into a MinimalJob
// This is needed for gang information to be extracted
type submitJobAdapter struct {
	*armadaevents.SubmitJob
}

// PriorityClassName is needed to fulfil the MinimalJob interface
func (j submitJobAdapter) PriorityClassName() string {
	podSpec := j.GetMainObject().GetPodSpec().GetPodSpec()
	if podSpec != nil {
		return podSpec.PriorityClassName
	}
	return ""
}

// Annotations is needed to fulfil the MinimalJob interface
func (j submitJobAdapter) Annotations() map[string]string {
	return j.GetObjectMeta().GetAnnotations()
}

// Gang is needed to fulfil the MinimalJob interface
func (j submitJobAdapter) Gang() *api.Gang {
	return j.SubmitJob.Gang
}

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

// normalizeGangInfo populates the Gang field from annotations for backward compatibility.
// If Gang is already set (new API style), it takes precedence. Otherwise, gang config
// is extracted from annotations (legacy style) and normalized into the Gang struct.
// Note: this only handles submitted values (gang ID, cardinality, node uniformity label name).
// The node uniformity label VALUE is computed by the scheduler later.
func normalizeGangInfo(msg *armadaevents.SubmitJob, _ configuration.SubmissionConfig) {
	if msg.Gang != nil {
		return
	}

	annotations := msg.GetObjectMeta().GetAnnotations()
	if annotations == nil {
		return
	}

	gangId := annotations[constants.GangIdAnnotation]
	if gangId == "" {
		return
	}

	gangInfo := &api.Gang{
		GangId: gangId,
	}

	if cardinalityStr := annotations[constants.GangCardinalityAnnotation]; cardinalityStr != "" {
		if val, err := strconv.ParseUint(cardinalityStr, 10, 32); err == nil {
			gangInfo.Cardinality = uint32(val)
		}
	}

	if labelName := annotations[constants.GangNodeUniformityLabelAnnotation]; labelName != "" {
		gangInfo.NodeUniformityLabelName = labelName
	}

	msg.Gang = gangInfo
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
// requests/limits for that particular resource. This can be used to e.g. ensure that all jobs define at least some
// ephemeral storage.
func defaultResource(spec *v1.PodSpec, config configuration.SubmissionConfig) {
	applyDefaults := func(containers []v1.Container) {
		for i := range containers {
			c := &containers[i]
			if c.Resources.Limits == nil {
				c.Resources.Limits = map[v1.ResourceName]resource.Quantity{}
			}
			if c.Resources.Requests == nil {
				c.Resources.Requests = map[v1.ResourceName]resource.Quantity{}
			}
			for res, val := range config.DefaultJobLimits {
				_, hasLimit := c.Resources.Limits[v1.ResourceName(res)]
				_, hasRequest := c.Resources.Requests[v1.ResourceName(res)]
				if !hasLimit && !hasRequest {
					c.Resources.Requests[v1.ResourceName(res)] = val
					c.Resources.Limits[v1.ResourceName(res)] = val
				}
			}
		}
	}

	applyDefaults(spec.Containers)
	applyDefaults(spec.InitContainers)
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
	annotations := msg.GetObjectMeta().GetAnnotations()
	if annotations == nil {
		return
	}
	if isGang(msg) {
		if _, ok := annotations[constants.GangNodeUniformityLabelAnnotation]; !ok {
			annotations[constants.GangNodeUniformityLabelAnnotation] = config.DefaultGangNodeUniformityLabel
		}
	}
}

// Default's gang jobs to have fail fast flag set to true if not set.
func defaultGangFailFastFlag(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig) {
	annotations := msg.GetObjectMeta().GetAnnotations()
	if annotations == nil {
		return
	}
	if isGang(msg) {
		if _, ok := annotations[constants.FailFastAnnotation]; !ok {
			annotations[constants.FailFastAnnotation] = "true"
		}
	}
}

func isGang(msg *armadaevents.SubmitJob) bool {
	adaptedJob := submitJobAdapter{msg}
	gangInfo, err := jobdb.GangInfoFromMinimalJob(adaptedJob)
	if err != nil {
		log.Errorf("job %s unexpectedly contained invalid gang info - %s", msg.JobId, err)
		return false
	}
	return gangInfo.IsGang()
}

// Add a gangId label if the gangId annotation is set.  We do this because labels are much faster to search on than
// annotations and a gang may want to hit the kubeapi to find its other gang members.
func addGangIdLabel(msg *armadaevents.SubmitJob, config configuration.SubmissionConfig) {
	if !config.AddGangIdLabel {
		return
	}

	gangId := msg.GetObjectMeta().GetAnnotations()[constants.GangIdAnnotation]
	if gangId != "" {
		labels := msg.GetObjectMeta().GetLabels()
		if labels == nil {
			labels = map[string]string{}
		}
		labels[constants.GangIdAnnotation] = gangId
		msg.GetObjectMeta().Labels = labels
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
	template(msg.GetObjectMeta().GetAnnotations(), msg.JobId)
	template(msg.GetObjectMeta().GetLabels(), msg.JobId)
}
