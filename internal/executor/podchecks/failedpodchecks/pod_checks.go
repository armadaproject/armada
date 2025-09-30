package failedpodchecks

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration/podchecks"
)

type RetryChecker interface {
	IsRetryable(pod *v1.Pod, podEvents []*v1.Event) (bool, string)
}

type PodRetryChecker struct {
	podStatusChecker             podStatusRetryChecker
	podEventChecker              eventRetryChecker
	failedContainerStatusChecker failedContainerStatusRetryChecker
}

func NewPodRetryChecker(config podchecks.FailedChecks) (*PodRetryChecker, error) {
	podStatusChecker, err := NewPodStatusChecker(config.PodStatuses)
	if err != nil {
		return nil, err
	}
	podEventChecker, err := NewPodEventsChecker(config.Events)
	if err != nil {
		return nil, err
	}
	failedContainerStatusChecker, err := NewFailedContainerStatusChecker(config.ContainerStatuses)
	if err != nil {
		return nil, err
	}

	return &PodRetryChecker{
		podEventChecker:              podEventChecker,
		podStatusChecker:             podStatusChecker,
		failedContainerStatusChecker: failedContainerStatusChecker,
	}, nil
}

func (f *PodRetryChecker) IsRetryable(pod *v1.Pod, podEvents []*v1.Event) (bool, string) {
	if hasStartedContainers(pod) {
		return f.failedContainerStatusChecker.IsRetryable(pod)
	}

	isRetryable, message := f.podEventChecker.IsRetryable(podEvents)

	if !isRetryable {
		isRetryable, message = f.podStatusChecker.IsRetryable(pod)
	}

	return isRetryable, message
}

func hasStartedContainers(pod *v1.Pod) bool {
	containers := pod.Status.ContainerStatuses
	containers = append(containers, pod.Status.InitContainerStatuses...)
	for _, container := range containers {
		if container.State.Running != nil ||
			container.State.Terminated != nil ||
			container.LastTerminationState.Running != nil ||
			container.LastTerminationState.Terminated != nil {
			return true
		}
	}
	return false
}
