package util

import (
	"fmt"
	"sort"
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

const failedMountReason = "FailedMount"
const failedFlexVolumeMountPrefix = "MountVolume.SetUp failed for volume"

const failedPullPrefix = "Failed to pull image"
const failedPullAndUnpack = "desc = failed to pull and unpack image"
const failedPullErrorResponse = "code = Unknown desc = Error response from daemon"

const pullImageReason = "Pulling"
const pullImageMessagePrefix = "Pulling image"
const pullImageBackoffReason = "BackOff"
const pullImageBackoffMessagePrefix = "Back-off pulling image"

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
			//This function is meant to be finding exit stauses of containers
			//Skip non-finished containers
			continue
		}
		status := &api.ContainerStatus{
			Name:  containerStatus.Name,
			Cause: api.Cause_Error,
		}
		if isOom(containerStatus) {
			status.Cause = api.Cause_OOM
		}
		status.ExitCode = containerStatus.State.Terminated.ExitCode
		status.Message = containerStatus.State.Terminated.Message
		status.Reason = containerStatus.State.Terminated.Reason
		returnStatuses = append(returnStatuses, status)
	}

	return returnStatuses
}

func isOom(containerStatus v1.ContainerStatus) bool {
	return containerStatus.State.Terminated != nil && containerStatus.State.Terminated.Reason == oomKilledReason
}

type PodStartupStatus int

const (
	Healthy PodStartupStatus = iota
	Unstable
	Unrecoverable
)

func DiagnoseStuckPod(pod *v1.Pod, podEvents []*v1.Event) (status PodStartupStatus, message string) {
	podStuckReason := ExtractPodStuckReason(pod)

	if hasUnrecoverableContainerState(pod) {
		return Unrecoverable, podStuckReason
	}

	if unrecoverable, event := hasUnrecoverableEvent(pod, podEvents); unrecoverable {
		return Unrecoverable, fmt.Sprintf("%s\n%s", podStuckReason, event.Message)
	}

	unexpectedWarningMessages := getUnexpectedWarningMessages(podEvents)
	if len(unexpectedWarningMessages) > 0 {
		eventMessage := "Warning Events:\n" + strings.Join(unexpectedWarningMessages, "\n")
		return Unstable, fmt.Sprintf("%s\n%s", podStuckReason, eventMessage)
	}

	if hasUnstableContainerStates(pod) {
		return Unstable, podStuckReason
	}
	return Healthy, podStuckReason
}

func hasUnrecoverableContainerState(pod *v1.Pod) bool {
	for _, containerStatus := range GetPodContainerStatuses(pod) {
		if containerStatus.State.Waiting != nil {
			waitingReason := containerStatus.State.Waiting.Reason
			if invalidImageNameStatesSet[waitingReason] {
				return true
			}
		}
	}
	return false
}

func hasUnrecoverableEvent(pod *v1.Pod, podEvents []*v1.Event) (bool, *v1.Event) {
	if isUnpullable, event := hasUnpullableImage(pod, podEvents); isUnpullable {
		return true, event
	}

	if isMountFailure, event := hasFailedMountEvent(podEvents); isMountFailure {
		return true, event
	}
	return false, nil
}

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

func hasUnpullableImage(pod *v1.Pod, podEvents []*v1.Event) (bool, *v1.Event) {
	for _, container := range GetPodContainerStatuses(pod) {
		if container.State.Waiting == nil {
			//Skip non-pending containers
			continue
		}
		if unpullable, event := isUnpullable(container.Image, podEvents); unpullable {
			return true, event
		}

	}
	return false, nil
}

func isUnpullable(image string, podEvents []*v1.Event) (bool, *v1.Event) {
	//Try to avoid marking an image unpullable if somehow the image has started pulling again
	if isCurrentlyPulling(image, podEvents) {
		return false, nil
	}
	for _, event := range podEvents {
		if event.Type == v1.EventTypeWarning && strings.HasPrefix(event.Message, failedPullPrefix) && strings.Contains(event.Message, image) {
			if strings.Contains(event.Message, failedPullAndUnpack) {
				return true, event
			}
			if strings.Contains(event.Message, failedPullErrorResponse) {
				return true, event
			}
		}
	}
	return false, nil
}

func isCurrentlyPulling(image string, podEvents []*v1.Event) bool {
	eventsForImage := []*v1.Event{}
	for _, event := range podEvents {
		if strings.Contains(event.Message, image) {
			eventsForImage = append(eventsForImage, event.DeepCopy())
		}
	}
	sort.Slice(podEvents, func(i, j int) bool {
		firstEventTime := podEvents[i].LastTimestamp.Time
		secondEventTime := podEvents[j].LastTimestamp.Time
		return firstEventTime.Before(secondEventTime)
	})
	indexOfPullingEvent := indexOfPullingEvent(eventsForImage)
	indexOfPullBackoffEvent := indexOfPullBackoffEvent(eventsForImage)
	return indexOfPullingEvent > 0 && indexOfPullingEvent > indexOfPullBackoffEvent
}

func indexOfPullingEvent(podEvents []*v1.Event) int {
	for i, event := range podEvents {
		if event.Reason == pullImageReason && strings.HasPrefix(event.Message, pullImageBackoffMessagePrefix) {
			return i
		}
	}
	return -1
}

func indexOfPullBackoffEvent(podEvents []*v1.Event) int {
	for i, event := range podEvents {
		if event.Type == v1.EventTypeNormal && event.Reason == pullImageBackoffReason && strings.HasPrefix(event.Message, pullImageBackoffMessagePrefix) {
			return i
		}
	}
	return -1
}

func hasFailedMountEvent(podEvents []*v1.Event) (bool, *v1.Event) {
	for _, event := range podEvents {
		if event.Type == v1.EventTypeWarning && event.Reason == failedMountReason && strings.HasPrefix(event.Message, failedFlexVolumeMountPrefix) {
			return true, event
		}
	}
	return false, nil
}

func getUnexpectedWarningMessages(podEvents []*v1.Event) []string {
	messages := []string{}
	for _, event := range podEvents {
		if event.Type == v1.EventTypeWarning && !expectedWarningsEventReasons[event.Reason] {
			messages = append(messages, fmt.Sprintf("%v: %v", event.Reason, event.Message))
		}
	}
	return messages
}
