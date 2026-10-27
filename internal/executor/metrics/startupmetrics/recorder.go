package startupmetrics

import (
	"strconv"

	v1 "k8s.io/api/core/v1"

	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

const untrackedContainer = "untracked"

// cachedUnknown labels an image we could not say either way about.
const cachedUnknown = "unknown"

// Recorder summarises the start-up breakdown of individual pods into metrics.
type Recorder struct {
	trackedInitContainers map[string]bool
}

func NewRecorder(config configuration.StartUpMetricsConfig) *Recorder {
	tracked := make(map[string]bool, len(config.TrackedInitContainers))
	for _, name := range config.TrackedInitContainers {
		tracked[name] = true
	}
	return &Recorder{trackedInitContainers: tracked}
}

func (r *Recorder) Record(pod *v1.Pod, info *armadaevents.PodStartUpInfo) {
	if r == nil || pod == nil || info == nil {
		return
	}

	queue := util.ExtractQueue(pod)
	pool := util.ExtractPool(pod)
	node := pod.Spec.NodeName

	issues := collectionIssues(pod, info)
	success := len(reportableIssues(issues)) == 0
	podStartUpInfoCollectedTotal.WithLabelValues(pool, strconv.FormatBool(success)).Inc()
	recordCollectionIssues(pod.Name, pool, issues)

	if total := interval(info.CreatedAt, info.StartedAt); total != nil {
		podStartUpDurationSeconds.WithLabelValues(queue, pool).Observe(total.Seconds())
		nodeStartUpDurationSeconds.WithLabelValues(node, pool).Observe(total.Seconds())
	}

	for _, step := range deriveSteps(info) {
		podStartUpStepDurationSeconds.WithLabelValues(queue, pool, step.name).Observe(step.duration.Seconds())
		nodeStartUpStepDurationSeconds.WithLabelValues(node, pool, step.name).Observe(step.duration.Seconds())
	}

	for _, container := range info.InitContainers {
		r.recordContainer(queue, pool, node, container, true)
	}
	for _, container := range info.Containers {
		r.recordContainer(queue, pool, node, container, false)
	}
}

// recordCollectionIssues detects if any fields were not collected
// Increments metrics + logs these issues
func recordCollectionIssues(podName, pool string, issues []collectionIssue) {
	if len(issues) == 0 {
		return
	}

	counted := make(map[string]bool, len(issues))
	for _, issue := range issues {
		if counted[issue.field] {
			continue
		}
		counted[issue.field] = true
		podStartUpInfoCollectionErrorTotal.WithLabelValues(pool, issue.field).Inc()
	}

	if message := incompleteBreakdownMessage(podName, issues); message != "" {
		log.Warn(message)
	}
}

func (r *Recorder) recordContainer(queue, pool, node string, container *armadaevents.ContainerStartUpInfo, initContainer bool) {
	if container == nil {
		return
	}

	if initContainer && !container.Sidecar {
		if run := interval(container.StartedAt, container.FinishedAt); run != nil {
			name := r.containerLabel(container.Name)
			podInitContainerRunDurationSeconds.WithLabelValues(queue, pool, name).Observe(run.Seconds())
			nodeInitContainerRunDurationSeconds.WithLabelValues(node, pool, name).Observe(run.Seconds())
		}
	}

	// Counted for every container, including one whose pull was not described at all, so that this
	// is the denominator for how often describing an image works.
	pullInfoSuccessfullyCollected := len(imagePullIssues(container)) == 0
	pull := container.ImagePull
	podImagePullInfoTotal.
		WithLabelValues(queue, pool, cachedLabelValue(pull), strconv.FormatBool(pullInfoSuccessfullyCollected)).
		Inc()
	if pull == nil {
		return
	}

	if pull.Duration != nil {
		podImagePullDurationSeconds.WithLabelValues(queue, pool).Observe(protoutil.ToStdDuration(pull.Duration).Seconds())
	}
	if pull.SizeBytes != nil {
		podImagePullSizeBytes.
			WithLabelValues(queue, pool, strconv.FormatBool(pull.Cached)).
			Observe(float64(pull.SizeBytes.Value))
	}
	if pull.WaitingTime != nil {
		waiting := protoutil.ToStdDuration(pull.WaitingTime).Seconds()
		podImagePullWaitingDurationSeconds.WithLabelValues(queue, pool).Observe(waiting)
		nodeImagePullWaitingDurationSeconds.WithLabelValues(node, pool).Observe(waiting)
	}
}

// cachedLabelValue says whether the image was already on the node, or unknown for a container whose
// pull was not described at all - where that is one of the things we failed to determine.
func cachedLabelValue(pull *armadaevents.ImagePullInfo) string {
	if pull == nil {
		return cachedUnknown
	}
	return strconv.FormatBool(pull.Cached)
}

// containerLabel keeps the init container label to the names the administrator configured, as a
// container's name comes from the job spec.
func (r *Recorder) containerLabel(name string) string {
	if r.trackedInitContainers[name] {
		return name
	}
	return untrackedContainer
}
