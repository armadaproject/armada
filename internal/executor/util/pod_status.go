package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/errormatch"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

var imagePullBackOffStatesSet = util.StringListToSet([]string{"ImagePullBackOff", "ErrImagePull"})

// preemptionByScheduler is the Kubernetes reason string set on the DisruptionTarget
// condition when the scheduler preempts a pod.
const preemptionByScheduler = "PreemptionByScheduler"

func ExtractPodFailedReason(pod *v1.Pod) string {
	if pod.Status.Message != "" {
		return pod.Status.Message
	}
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

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
	if pod.Status.Reason == errormatch.ConditionEvicted {
		return armadaevents.KubernetesReason_Evicted
	}
	if pod.Status.Reason == errormatch.ConditionDeadlineExceeded {
		return armadaevents.KubernetesReason_DeadlineExceeded
	}

	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	for _, containerStatus := range containerStatuses {
		if isOom(containerStatus) {
			return armadaevents.KubernetesReason_OOM
		}
	}
	return armadaevents.KubernetesReason_AppError
}

func ExtractPodExitCodes(pod *v1.Pod) map[string]int32 {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	exitCodes := map[string]int32{}

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated != nil {
			exitCodes[containerStatus.Name] = containerStatus.State.Terminated.ExitCode
		}
	}

	return exitCodes
}

func ExtractFailedPodContainerStatuses(pod *v1.Pod, clusterId string) []*armadaevents.ContainerError {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

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

// ExtractFailureInfo builds a FailureInfo proto from pod status, pod check results,
// and category labels. It determines the failure condition from pod-level signals
// and extracts the exit code and termination message from the first failed container.
func ExtractFailureInfo(pod *v1.Pod, podCheckRetryable bool, podCheckMessage string, categories []string) *armadaevents.FailureInfo {
	info := &armadaevents.FailureInfo{
		PodCheckRetryable: podCheckRetryable,
		PodCheckMessage:   podCheckMessage,
		Categories:        categories,
	}

	if pod == nil {
		return info
	}

	// Pod-level condition; may be overridden by container-level OOM below.
	if isPreempted(pod) {
		info.Condition = armadaevents.FailureCondition_FAILURE_CONDITION_PREEMPTED
	} else if pod.Status.Reason == errormatch.ConditionEvicted {
		info.Condition = armadaevents.FailureCondition_FAILURE_CONDITION_EVICTED
	} else if pod.Status.Reason == errormatch.ConditionDeadlineExceeded {
		info.Condition = armadaevents.FailureCondition_FAILURE_CONDITION_DEADLINE_EXCEEDED
	} else {
		info.Condition = armadaevents.FailureCondition_FAILURE_CONDITION_USER_ERROR
	}

	// Single pass: find OOM condition and first failed container's exit code.
	// Only upgrade to OOM when the pod-level condition is USER_ERROR.
	// Preempted/evicted/deadline pods may OOM during shutdown, but the
	// pod-level signal is the true cause.
	for _, cs := range GetPodContainerStatuses(pod) {
		if cs.State.Terminated == nil {
			continue
		}
		if cs.State.Terminated.Reason == errormatch.ConditionOOMKilled &&
			info.Condition == armadaevents.FailureCondition_FAILURE_CONDITION_USER_ERROR {
			info.Condition = armadaevents.FailureCondition_FAILURE_CONDITION_OOM_KILLED
		}
		if info.ExitCode == 0 && cs.State.Terminated.ExitCode != 0 {
			info.ExitCode = cs.State.Terminated.ExitCode
			info.TerminationMessage = cs.State.Terminated.Message
		}
	}

	return info
}

// isPreempted checks if the pod was preempted by looking for the
// DisruptionTarget condition with "PreemptionByScheduler" reason.
func isPreempted(pod *v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.DisruptionTarget && cond.Reason == preemptionByScheduler {
			return true
		}
	}
	return false
}

func isOom(containerStatus v1.ContainerStatus) bool {
	return containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == errormatch.ConditionOOMKilled
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
