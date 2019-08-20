package util

import (
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	"time"
)

func CreateEventMessageForCurrentState(pod *v1.Pod, clusterId string) (*api.EventMessage, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		return api.Wrap(&api.JobPendingEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		})

	case v1.PodRunning:
		return api.Wrap(&api.JobRunningEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		})

	case v1.PodFailed:
		return api.Wrap(&api.JobFailedEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		})

	case v1.PodSucceeded:
		return api.Wrap(&api.JobSucceededEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		})

	default:
		return new(api.EventMessage), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}
