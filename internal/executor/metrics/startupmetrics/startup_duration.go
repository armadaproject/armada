package startupmetrics

import (
	"time"

	"github.com/armadaproject/armada/pkg/armadaevents"
)

type startUpDurations struct {
	total *time.Duration
	// The total without the run time of the init containers the administrator did not configure
	// This is useful for understanding how much time the platform took to start the pod.
	withoutUntrackedInitContainers *time.Duration
	// The total without the run time of any init container, leaving the Kubernetes overhead.
	withoutInitContainers *time.Duration
}

func deriveStartUpDurations(info *armadaevents.PodStartUpInfo, trackedInitContainers map[string]bool) startUpDurations {
	if info == nil {
		return startUpDurations{}
	}
	total := interval(info.CreatedAt, info.StartedAt)
	if total == nil {
		return startUpDurations{}
	}

	trackedInitContainerDuration := time.Duration(0)
	unTrackedInitContainerDuration := time.Duration(0)
	for _, container := range info.InitContainers {
		// A sidecar is still running when the pod starts, so it has no run time to take off.
		if container == nil || container.Sidecar {
			continue
		}
		run := interval(container.StartedAt, container.FinishedAt)
		if run == nil {
			continue
		}

		isTracked := trackedInitContainers[container.Name]
		if isTracked {
			trackedInitContainerDuration += *run
		} else {
			unTrackedInitContainerDuration += *run
		}
	}

	durations := startUpDurations{
		total:                          total,
		withoutUntrackedInitContainers: remaining(*total, unTrackedInitContainerDuration),
		withoutInitContainers:          remaining(*total, unTrackedInitContainerDuration+trackedInitContainerDuration),
	}
	return durations
}

func remaining(total time.Duration, excluded time.Duration) *time.Duration {
	if excluded > total {
		return nil
	}
	return new(total - excluded)
}
