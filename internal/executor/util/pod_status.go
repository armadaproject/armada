package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var imagePullBackOffStatesSet = util.StringListToSet([]string{"ImagePullBackOff", "ErrImagePull"})

const (
	oomKilledReason  = "OOMKilled"
	evictedReason    = "Evicted"
	deadlineExceeded = "DeadlineExceeded"
)

// TODO: Need to detect pod preemption. So that job failed events can include a string indicating a pod was preempted.
// We need this so that whatever system submitted the job knows the job was preempted.

// allContainerStatuses returns all container statuses (regular and init) for a pod.
func allContainerStatuses(pod *v1.Pod) []v1.ContainerStatus {
	statuses := pod.Status.ContainerStatuses
	return append(statuses, pod.Status.InitContainerStatuses...)
}

func ExtractPodFailedReason(pod *v1.Pod) string {
	if pod.Status.Message != "" {
		return pod.Status.Message
	}
	containerStatuses := allContainerStatuses(pod)

	failedMessage := ""

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			terminatedState := containerStatus.State.Terminated
			failedMessage += fmt.Sprintf(
				"Container %s failed with exit code %d because %s: %s\n",
				containerStatus.Name,
				terminatedState.ExitCode,
				terminatedState.Reason,
				terminatedState.Message,
			)
		}
	}

	return failedMessage
}

func ExtractPodFailureCause(pod *v1.Pod) armadaevents.KubernetesReason {
	if pod.Status.Reason == evictedReason {
		return armadaevents.KubernetesReason_Evicted
	}
	if pod.Status.Reason == deadlineExceeded {
		return armadaevents.KubernetesReason_DeadlineExceeded
	}

	for _, containerStatus := range allContainerStatuses(pod) {
		if isOom(containerStatus) {
			return armadaevents.KubernetesReason_OOM
		}
	}
	return armadaevents.KubernetesReason_AppError
}

func ExtractPodExitCodes(pod *v1.Pod) map[string]int32 {
	exitCodes := map[string]int32{}
	for _, containerStatus := range allContainerStatuses(pod) {
		if containerStatus.State.Terminated != nil {
			exitCodes[containerStatus.Name] = containerStatus.State.Terminated.ExitCode
		}
	}

	return exitCodes
}

func ExtractFailedPodContainerStatuses(pod *v1.Pod, clusterId string) []*armadaevents.ContainerError {
	containerStatuses := allContainerStatuses(pod)
	returnStatuses := make([]*armadaevents.ContainerError, 0, len(containerStatuses))

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated == nil {
			// This function is meant to be finding exit stauses of containers
			// Skip non-finished containers
			continue
		}

		containerInfo := &armadaevents.ContainerError{
			ExitCode: containerStatus.State.Terminated.ExitCode,
			Message:  containerStatus.State.Terminated.Message,
			Reason:   containerStatus.State.Terminated.Reason,
			ObjectMeta: &armadaevents.ObjectMeta{
				ExecutorId:   clusterId,
				Namespace:    pod.Namespace,
				Name:         containerStatus.Name,
				KubernetesId: "", // only the id of the pod is stored in the failed message
			},
		}

		containerInfo.KubernetesReason = armadaevents.KubernetesReason_AppError
		if isOom(containerStatus) {
			containerInfo.KubernetesReason = armadaevents.KubernetesReason_OOM
		}

		returnStatuses = append(returnStatuses, containerInfo)
	}
	return returnStatuses
}

func isOom(containerStatus v1.ContainerStatus) bool {
	return containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == oomKilledReason
}

// ExtractFailureInfo extracts structured failure information from a pod for retry policy evaluation.
// This provides the scheduler with the information needed to make fine-grained retry decisions.
func ExtractFailureInfo(pod *v1.Pod, retryable bool, message string) *armadaevents.FailureInfo {
	info := &armadaevents.FailureInfo{
		PodCheckRetryable: retryable,
		PodCheckMessage:   message,
	}

	// Determine the high-level failure condition
	info.Condition = mapKubernetesReasonToCondition(ExtractPodFailureCause(pod), pod)

	// Extract exit code and termination message from the first failed container
	for _, containerStatus := range allContainerStatuses(pod) {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			info.ExitCode = containerStatus.State.Terminated.ExitCode
			info.TerminationMessage = containerStatus.State.Terminated.Message
			break // Use first failed container's exit code and message
		}
	}

	return info
}

// mapKubernetesReasonToCondition maps KubernetesReason to FailureCondition.
func mapKubernetesReasonToCondition(reason armadaevents.KubernetesReason, pod *v1.Pod) armadaevents.FailureCondition {
	switch reason {
	case armadaevents.KubernetesReason_Evicted:
		return armadaevents.FailureCondition_FAILURE_CONDITION_EVICTED
	case armadaevents.KubernetesReason_OOM:
		return armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED
	case armadaevents.KubernetesReason_DeadlineExceeded:
		return armadaevents.FailureCondition_FAILURE_CONDITION_DEADLINE_EXCEEDED
	default:
		// Check for preemption (pod was deleted with specific conditions)
		if isPreempted(pod) {
			return armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED
		}
		return armadaevents.FailureCondition_FAILURE_CONDITION_USER_ERROR
	}
}

// isPreempted checks if a pod was preempted.
// Preemption is detected by checking for "Preempting"/"Preempted" reasons
// or the DisruptionTarget condition.
func isPreempted(pod *v1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Reason == "Preempting" || condition.Reason == "Preempted" {
			return true
		}
		if condition.Type == "DisruptionTarget" && condition.Status == v1.ConditionTrue {
			return true
		}
	}
	return false
}

type PodStartupStatus int

func hasUnstableContainerStates(pod *v1.Pod) bool {
	for _, containerStatus := range GetPodContainerStatuses(pod) {
		if containerStatus.State.Waiting != nil {
			waitingReason := containerStatus.State.Waiting.Reason
			if imagePullBackOffStatesSet[waitingReason] {
				return true
			}
		}
	}
	return false
}
