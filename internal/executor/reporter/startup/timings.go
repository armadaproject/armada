package startup

import (
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// Timings provides a breakdown where time was spent during a pod startup from Pending -> Running
type Timings struct {
	extraSteps []compiledStep
}

func NewTimings(config configuration.StartUpTimingsConfig) (*Timings, error) {
	steps, err := compileSteps(config.ExtraSteps)
	if err != nil {
		return nil, err
	}
	return &Timings{extraSteps: steps}, nil
}

func (t *Timings) Measure(pod *v1.Pod, podEvents []*v1.Event) *armadaevents.PodStartUpInfo {
	if t == nil {
		return nil
	}

	created := GetPodCreationTime(pod)
	startedAt := GetLatestAppContainerStartTime(pod)

	containerDetails := getContainerStartUpDetails(pod, podEvents)
	info := &armadaevents.PodStartUpInfo{
		CreatedAt:             toTimestamp(created),
		ScheduledAt:           toTimestamp(GetPodScheduledTime(pod, podEvents)),
		KubeletAcknowledgedAt: toTimestamp(GetKubeletAcknowledgedTime(pod)),
		SandboxReadyAt:        toTimestamp(GetSandboxReadyTime(pod)),
		StartedAt:             toTimestamp(startedAt),
		InitContainers:        containerDetails.initContainers,
		Containers:            containerDetails.containers,
		ExtraSteps:            measureExtraSteps(t.extraSteps, podEvents),
	}

	if info.IsEmpty() {
		return nil
	}
	return info
}
