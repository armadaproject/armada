package util

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

var expectedWarningsEventReasons = util.StringListToSet([]string{
	// As Armada sometimes over subscribe cluster it is expected that some pods fails to schedule
	"FailedScheduling",
})
var imagePullBackOffStatesSet = util.StringListToSet([]string{"ImagePullBackOff", "ErrImagePull"})
var invalidImageNameStatesSet = util.StringListToSet([]string{"InvalidImageName"})

const oomKilledReason = "OOMKilled"
const evictedReason = "Evicted"
const deadlineExceeded = "DeadlineExceeded"

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
	if pod.Status.Message != "" {
		return pod.Status.Message
	}
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

func ExtractPodFailedCause(pod *v1.Pod) api.Cause {
	if pod.Status.Reason == evictedReason {
		return api.Cause_Evicted
	}
	if pod.Status.Reason == deadlineExceeded {
		return api.Cause_DeadlineExceeded
	}

	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	for _, containerStatus := range containerStatuses {
		if isOom(containerStatus) {
			return api.Cause_OOM
		}
	}
	return api.Cause_Error
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

func ExtractFailedPodContainerStatuses(pod *v1.Pod) []*api.ContainerStatus {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	returnStatuses := make([]*api.ContainerStatus, 0, len(containerStatuses))

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Terminated == nil {
			//For some errors, Kubernetes confusingly leaves the containers in state running even though they are killed on the host
			//Skip them to avoid confusion
			continue
		}
		status := &api.ContainerStatus{
			Name:  containerStatus.Name,
			Cause: api.Cause_Error,
		}
		if isOom(containerStatus) {
			status.Cause = api.Cause_OOM
		}
		if containerStatus.State.Terminated != nil {
			status.ExitCode = containerStatus.State.Terminated.ExitCode
			status.Message = containerStatus.State.Terminated.Message
			status.Reason = containerStatus.State.Terminated.Reason
		}
		returnStatuses = append(returnStatuses, status)
	}

	return returnStatuses
}

func isOom(containerStatus v1.ContainerStatus) bool {
	return containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == oomKilledReason
}

func DiagnoseStuckPod(pod *v1.Pod, podEvents []*v1.Event) (retryable bool, message string) {
	messages := []string{}
	for _, event := range podEvents {
		if event.Type == v1.EventTypeWarning && !expectedWarningsEventReasons[event.Reason] {
			messages = append(messages, fmt.Sprintf("%v: %v", event.Reason, event.Message))
		}
	}

	podStuckReason := ExtractPodStuckReason(pod)

	if len(messages) > 0 {
		eventMessage := "Warning Events:\n" + strings.Join(messages, "\n")
		return false, fmt.Sprintf("%s\n%s", podStuckReason, eventMessage)
	}

	return ContainersAreRetryable(pod), podStuckReason
}

func ContainersAreRetryable(pod *v1.Pod) bool {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)

	for _, containerStatus := range containerStatuses {
		if containerStatus.State.Waiting != nil {
			waitingReason := containerStatus.State.Waiting.Reason
			if imagePullBackOffStatesSet[waitingReason] {
				return false
			}
			if invalidImageNameStatesSet[waitingReason] {
				return false
			}
		}
	}

	return true
}
