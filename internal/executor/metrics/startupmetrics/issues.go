package startupmetrics

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Field names reported by pod_start_up_info_collection_error_total. Every part of the breakdown is
// optional, as each is derived either from a Kubernetes field that may not be populated or from a
// kubelet message that may change format, so each needs to be visible when it stops arriving.
const (
	fieldScheduledAt           = "scheduled_at"
	fieldKubeletAcknowledgedAt = "kubelet_acknowledged_at"
	fieldSandboxReadyAt        = "sandbox_ready_at"
	fieldStartedAt             = "started_at"
	fieldInitContainers        = "init_containers"
	fieldContainers            = "containers"
	fieldImagePull             = "image_pull"
	fieldImagePullDetails      = "image_pull_details"
	// A cached pull is told apart from a real one, as the two fail for different reasons: a real
	// pull's details come from parsing the kubelet's message, a cached pull's size is resolved
	// separately and best effort.
	fieldCachedImagePullSize = "cached_image_pull_size"
)

// collectionIssue is one thing about a pod's start up that could not be determined. The field is
// what the metric counts, deliberately coarse; the detail is which container and which instant it
// actually was, which is logged rather than labelled.
type collectionIssue struct {
	field  string
	detail string
}

func (i collectionIssue) String() string {
	if i.detail == "" {
		return i.field
	}
	return fmt.Sprintf("%s (%s)", i.field, i.detail)
}

// collectionIssues lists everything about the breakdown that could not be determined. A problem
// with any one of a pod's containers is reported as a problem with that kind of container: what the
// metric has to answer is whether collection still works, and splitting it per container and per
// instant would only multiply the series.
//
// created_at is not reported: it comes from the pod's own creationTimestamp, so a pod we are
// measuring at all always has it.
//
// Configured extra steps are not reported either. A step may apply to only a fraction of jobs - a
// shape of job, a pool, a label - so a step that matched no event is not an error unless we know
// it was expected of this pod, which is knowledge the configuration does not carry yet. The steps
// that did match are still measured as steps.
func collectionIssues(pod *v1.Pod, info *armadaevents.PodStartUpInfo) []collectionIssue {
	issues := &issueList{}

	issues.report(info.ScheduledAt == nil, fieldScheduledAt, "")
	issues.report(info.KubeletAcknowledgedAt == nil, fieldKubeletAcknowledgedAt, "")
	issues.report(info.SandboxReadyAt == nil, fieldSandboxReadyAt, "")
	issues.report(info.StartedAt == nil, fieldStartedAt, "")

	issues.reportContainers(fieldInitContainers, pod.Spec.InitContainers, info.InitContainers, true)
	issues.reportContainers(fieldContainers, pod.Spec.Containers, info.Containers, false)

	return issues.found
}

// issueList accumulates what was found wrong with one pod's breakdown.
type issueList struct {
	found []collectionIssue
}

// report records an issue when isMissing. The detail may be empty, for a field whose name already
// says everything there is to say.
func (l *issueList) report(isMissing bool, field, detail string) {
	if isMissing {
		l.found = append(l.found, collectionIssue{field: field, detail: detail})
	}
}

// reportContainers covers the containers of one kind, and the image pull of each: a pull is the
// kubelet doing the same thing whichever kind of container it is for, so both kinds report under
// the one image pull field.
func (l *issueList) reportContainers(
	field string,
	inSpec []v1.Container,
	described []*armadaevents.ContainerStartUpInfo,
	initContainers bool,
) {
	l.report(len(described) < len(inSpec), field,
		fmt.Sprintf("described %d of the pod's %d", len(described), len(inSpec)))

	for _, container := range described {
		if container == nil {
			l.report(true, field, "a container was described as nothing at all")
			continue
		}
		l.report(container.CreatedAt == nil, field, fmt.Sprintf("%s has no creation time", container.Name))
		l.report(container.StartedAt == nil, field, fmt.Sprintf("%s has no start time", container.Name))
		// Only an init container is expected to have finished, and a sidecar is still running when
		// the pod reaches Running, so neither an app container nor a sidecar is checked.
		l.report(initContainers && !container.Sidecar && container.FinishedAt == nil, field,
			fmt.Sprintf("%s has no finish time", container.Name))

		l.found = append(l.found, imagePullIssues(container)...)
	}
}

// imagePullIssues is what could not be determined about one container's image pull. A pod has one
// per container, so these are counted per image as well as per pod - pod_image_pull_info_total
// carries the per image view.
//
// A cached pull is expected to carry only a size: the kubelet's "already present on machine"
// message has no duration or waiting time to parse, and the size is resolved separately and best
// effort, so it is the part that can fail on its own.
func imagePullIssues(container *armadaevents.ContainerStartUpInfo) []collectionIssue {
	issues := &issueList{}

	pull := container.ImagePull
	issues.report(pull == nil, fieldImagePull, fmt.Sprintf("%s has no image pull described", container.Name))
	if pull == nil {
		return issues.found
	}

	if pull.Cached {
		issues.report(pull.SizeBytes == nil, fieldCachedImagePullSize,
			fmt.Sprintf("%s cached pull has no size", container.Name))
		return issues.found
	}

	issues.report(pull.Duration == nil, fieldImagePullDetails, fmt.Sprintf("%s pull has no duration", container.Name))
	issues.report(pull.WaitingTime == nil, fieldImagePullDetails, fmt.Sprintf("%s pull has no waiting time", container.Name))
	issues.report(pull.SizeBytes == nil, fieldImagePullDetails, fmt.Sprintf("%s pull has no size", container.Name))
	return issues.found
}

// reportableIssues drops the issues not worth acting on, which is what decides both whether a
// breakdown counts as complete and whether it is worth warning about.
//
// sandbox_ready_at is dropped: it comes from the PodReadyToStartContainers condition, which only
// Kubernetes 1.33 and above sets, so on an older cluster it is missing for every pod, which would
// make every breakdown incomplete and warn once per pod for the life of the cluster. It is still
// counted per field, which is where an operator sees that the cluster does not report it.
func reportableIssues(issues []collectionIssue) []collectionIssue {
	return armadaslices.Filter(issues, func(issue collectionIssue) bool {
		return issue.field != fieldSandboxReadyAt
	})
}

// incompleteBreakdownMessage is what to warn an operator about a pod, or empty when there is
// nothing worth warning about. It is where the detail of each issue is reported, as the metrics
// carry only the coarse field.
func incompleteBreakdownMessage(podName string, issues []collectionIssue) string {
	reportable := reportableIssues(issues)
	if len(reportable) == 0 {
		return ""
	}

	described := armadaslices.Map(reportable, func(issue collectionIssue) string { return issue.String() })
	return fmt.Sprintf("Start up breakdown of pod %s is incomplete: %s", podName, strings.Join(described, ", "))
}
