package util

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/armadaevents"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/common/util"
)

var imagePullBackOffStatesSet = util.StringListToSet([]string{"ImagePullBackOff", "ErrImagePull"})

const (
	oomKilledReason  = "OOMKilled"
	evictedReason    = "Evicted"
	deadlineExceeded = "DeadlineExceeded"
)

// TODO: Need to detect pod preemption. So that job failed events can include a string indicating a pod was preempted.
// We need this so that whatever system submitted the job knows the job was preempted.

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
	if pod.Status.Reason == evictedReason {
		return armadaevents.KubernetesReason_Evicted
	}
	if pod.Status.Reason == deadlineExceeded {
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

func isOom(containerStatus v1.ContainerStatus) bool {
	return containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == oomKilledReason
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
