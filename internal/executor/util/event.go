package util

import (
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	"time"
)

func CreateEventForCurrentState(pod *v1.Pod, clusterId string) (api.Event, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		return &api.JobPendingEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		}, nil
	case v1.PodRunning:
		return &api.JobRunningEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		}, nil
	case v1.PodFailed:
		return &api.JobFailedEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		}, nil
	case v1.PodSucceeded:
		return &api.JobSucceededEvent{
			JobId:     pod.Labels[domain.JobId],
			JobSetId:  pod.Labels[domain.JobSetId],
			Queue:     pod.Labels[domain.Queue],
			Created:   time.Now(),
			ClusterId: clusterId,
		}, nil
	default:
		return *new(api.Event), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}

func CreateJobUnableToScheduleEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobUnableToScheduleEvent{
		JobId:     pod.Labels[domain.JobId],
		JobSetId:  pod.Labels[domain.JobSetId],
		Queue:     pod.Labels[domain.Queue],
		Created:   time.Now(),
		ClusterId: clusterId,
		Reason:    reason,
	}
}

func CreateJobFailedEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobFailedEvent{
		JobId:     pod.Labels[domain.JobId],
		JobSetId:  pod.Labels[domain.JobSetId],
		Queue:     pod.Labels[domain.Queue],
		Created:   time.Now(),
		ClusterId: clusterId,
		Reason:    reason,
	}
}
