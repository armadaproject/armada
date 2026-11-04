package startup

import (
	"time"

	"github.com/gogo/protobuf/types"
	v1 "k8s.io/api/core/v1"

	protoutil "github.com/armadaproject/armada/internal/common/proto"
)

func GetPodCreationTime(pod *v1.Pod) *time.Time {
	return &pod.CreationTimestamp.Time
}

// GetPodScheduledTime is when kube-scheduler bound the pod to a node. Armada has already chosen the
// node by this point, pinning it with the NodeIdLabel nodeSelector, so this measures the bind rather
// than the placement decision.
//
// kube-scheduler's Scheduled event is the only accurate source, so it is preferred, fallback to condition
func GetPodScheduledTime(pod *v1.Pod, podEvents []*v1.Event) *time.Time {
	if scheduled := firstEventTimeWithReason(podEvents, scheduledReason); scheduled != nil {
		return scheduled
	}
	return getPodConditionTime(pod, v1.PodScheduled)
}

// GetKubeletAcknowledgedTime is status.startTime, which the API documents as set when the kubelet
// acknowledged the pod - before it pulled any image.
func GetKubeletAcknowledgedTime(pod *v1.Pod) *time.Time {
	if pod.Status.StartTime == nil || pod.Status.StartTime.IsZero() {
		return nil
	}
	return &pod.Status.StartTime.Time
}

// GetSandboxReadyTime is when the pod's sandbox existed with its networking configured.
// Returns nil when the PodReadyToStartContainersCondition feature gate is off
func GetSandboxReadyTime(pod *v1.Pod) *time.Time {
	return getPodConditionTime(pod, v1.PodReadyToStartContainers)
}

// GetLatestAppContainerStartTime is the point the pod can be said to have started: the last of its
// app containers to start for the first time, which is when the phase flips to Running. Init
// containers and sidecars are excluded, since both necessarily start earlier.
func GetLatestAppContainerStartTime(pod *v1.Pod) *time.Time {
	var latest *time.Time
	for _, status := range pod.Status.ContainerStatuses {
		startedAt := GetContainerStartedTime(status)
		if startedAt == nil {
			continue
		}
		if latest == nil || startedAt.After(*latest) {
			latest = startedAt
		}
	}
	return latest
}

// GetContainerStartedTime is the earliest start the container's status still reports.
//
// A container that has restarted describes its current incarnation in State and the one before it
// in LastTerminationState. The earlier of the two belongs on the pod's start-up timeline, because
// what the pod waited for was the container first starting, not the restart. Only the immediately
// previous incarnation is retained, so a container that has restarted more than once reports later
// than it actually first started - restartCount on the status is the only hint that happened.
func GetContainerStartedTime(status v1.ContainerStatus) *time.Time {
	return earliest(
		containerStateStartTime(status.LastTerminationState),
		containerStateStartTime(status.State),
	)
}

// containerStateStartTime is when the container described by one state began running, from whichever
// field that state carries it in. A waiting container has never run and reports nothing.
func containerStateStartTime(state v1.ContainerState) *time.Time {
	if finished := state.Terminated; finished != nil && !finished.StartedAt.IsZero() {
		return &finished.StartedAt.Time
	}
	if running := state.Running; running != nil && !running.StartedAt.IsZero() {
		return &running.StartedAt.Time
	}
	return nil
}

func earliest(times ...*time.Time) *time.Time {
	var found *time.Time
	for _, candidate := range times {
		if candidate == nil {
			continue
		}
		if found == nil || candidate.Before(*found) {
			found = candidate
		}
	}
	return found
}

func GetContainerFinishedTime(status v1.ContainerStatus) *time.Time {
	finished := status.State.Terminated
	if finished == nil || finished.FinishedAt.IsZero() {
		return nil
	}
	return &finished.FinishedAt.Time
}

func getPodConditionTime(pod *v1.Pod, conditionType v1.PodConditionType) *time.Time {
	for _, condition := range pod.Status.Conditions {
		if condition.Type != conditionType {
			continue
		}
		if condition.Status != v1.ConditionTrue || condition.LastTransitionTime.IsZero() {
			return nil
		}
		return &condition.LastTransitionTime.Time
	}
	return nil
}

func toTimestamp(t *time.Time) *types.Timestamp {
	if t == nil {
		return nil
	}
	return protoutil.ToTimestamp(*t)
}
