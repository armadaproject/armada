package startupmetrics

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// These fields feed into the metric pod_start_up_info_collection_error_total.
const (
	fieldScheduledAt           = "scheduled_at"
	fieldKubeletAcknowledgedAt = "kubelet_acknowledged_at"
	fieldSandboxReadyAt        = "sandbox_ready_at"
	fieldStartedAt             = "started_at"
	fieldInitContainers        = "init_containers"
	fieldContainers            = "containers"
	fieldImagePull             = "image_pull"
	fieldImagePullDetails      = "image_pull_details"
	fieldCachedImagePullSize   = "cached_image_pull_size"
)

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

// collectionIssues lists all issues that occurred during collection of pod start up info
// The aim here is to monitor what fields we're failing to monitor properly and increment a metric when the field isn't measured
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

type issueList struct {
	found []collectionIssue
}

func (l *issueList) report(isMissing bool, field, detail string) {
	if isMissing {
		l.found = append(l.found, collectionIssue{field: field, detail: detail})
	}
}

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

func imagePullIssues(container *armadaevents.ContainerStartUpInfo) []collectionIssue {
	issues := &issueList{}

	pull := container.ImagePull
	issues.report(pull == nil, fieldImagePull, fmt.Sprintf("%s has no image pull described", container.Name))
	if pull == nil {
		return issues.found
	}

	if pull.Cached {
		// A cached pull is expected to carry only a size
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

func incompleteBreakdownMessage(podName string, issues []collectionIssue) string {
	reportable := reportableIssues(issues)
	if len(reportable) == 0 {
		return ""
	}

	described := armadaslices.Map(reportable, func(issue collectionIssue) string { return issue.String() })
	return fmt.Sprintf("Start up breakdown of pod %s is incomplete: %s", podName, strings.Join(described, ", "))
}
