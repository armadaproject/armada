package util

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

var imagePullBackOffStatesSet = map[string]bool{"ImagePullBackOff": true, "ErrImagePull": true}

func ExtractPodStuckReason(pod *v1.Pod) string {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	stuckMessage := ""

	for _, containerStatus := range containerStatuses {
		if !containerStatus.Ready && containerStatus.State.Waiting != nil {
			waitingState := containerStatus.State.Waiting
			stuckMessage += fmt.Sprintf("Container %s failed to start because %s: %s\n", containerStatus.Name, waitingState.Reason, waitingState.Message)
		}
	}

	if stuckMessage == "" && pod.Status.NominatedNodeName == "" {
		stuckMessage += "Pod became stuck due to insufficient space on any node to schedule the pod.\n"
	}

	return stuckMessage
}

func ExtractPodFailedReason(pod *v1.Pod) string {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	failedMessage := ""

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated != nil && containerStatus.State.Terminated.ExitCode != 0 {
			terminatedState := containerStatus.State.Terminated
			failedMessage += fmt.Sprintf("Container %s failed with exit code %d because %s: %s\n", containerStatus.Name, terminatedState.ExitCode, terminatedState.Reason, terminatedState.Message)
		}
	}

	return failedMessage
}

func IsRetryable(pod *v1.Pod) bool {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Waiting != nil {
			waitingReason := containerStatus.State.Waiting.Reason
			if imagePullBackOffStatesSet[waitingReason] {
				return false
			}
		}
	}

	return true
}
