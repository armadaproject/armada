package startup

import (
	"time"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type containerStartUpDetails struct {
	initContainers []*armadaevents.ContainerStartUpInfo
	containers     []*armadaevents.ContainerStartUpInfo
}

func getContainerStartUpDetails(pod *v1.Pod, podEvents []*v1.Event) containerStartUpDetails {
	eventsByContainer := groupEventsByContainer(podEvents)

	var details containerStartUpDetails
	for _, status := range pod.Status.InitContainerStatuses {
		details.initContainers = append(details.initContainers, getContainerStartupInfo(
			status,
			eventsByContainer[initContainerKey(status.Name)],
			isSidecarInitContainer(pod, status.Name),
		))
	}
	for _, status := range pod.Status.ContainerStatuses {
		details.containers = append(details.containers, getContainerStartupInfo(
			status,
			eventsByContainer[appContainerFieldKey(status.Name)],
			false,
		))
	}
	return details
}

func getContainerStartupInfo(
	status v1.ContainerStatus,
	events containerEvents,
	isSidecar bool,
) *armadaevents.ContainerStartUpInfo {
	return &armadaevents.ContainerStartUpInfo{
		Name:      status.Name,
		CreatedAt: toTimestamp(events.timeOf(createdReason)),
		// Prefer container's own status is over the Started event:
		//  - It is a pod field, so it cannot be lost to the event spam filte
		//  - The event is the fallback for a container whose status has not reported a start time.
		StartedAt:  toTimestamp(firstKnown(GetContainerStartedTime(status), events.timeOf(startedReason))),
		FinishedAt: toTimestamp(GetContainerFinishedTime(status)),
		ImagePull:  events.getImagePullInfo(),
		// This is so downstream systems can determine which containers really needed to run before the pod could start
		// Sidecar containers run alongside the main containers so aren't involved in the pod's startup
		Sidecar: isSidecar,
	}
}

func firstKnown(times ...*time.Time) *time.Time {
	for _, candidate := range times {
		if candidate != nil {
			return candidate
		}
	}
	return nil
}

// isSidecarInitContainer reports whether an init container is a native sidecar
// Determined by restartPolicy Always
// This function assumes you're passing in an init containers name
func isSidecarInitContainer(pod *v1.Pod, name string) bool {
	for _, container := range pod.Spec.InitContainers {
		if container.Name != name {
			continue
		}
		return container.RestartPolicy != nil && *container.RestartPolicy == v1.ContainerRestartPolicyAlways
	}
	return false
}
