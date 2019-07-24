package util

import (
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	"time"
)

func CreateEventMessageForCurrentState(pod *v1.Pod) (*api.EventMessage, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		pending := api.JobPendingEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		pendingMessage := api.EventMessage_Pending{
			Pending: &pending,
		}

		message := api.EventMessage{
			Events: &pendingMessage,
		}

		return &message, nil

	case v1.PodRunning:
		running := api.JobRunningEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		runningMessage := api.EventMessage_Running{
			Running: &running,
		}

		message := api.EventMessage{
			Events: &runningMessage,
		}

		return &message, nil

	case v1.PodFailed:
		failed := api.JobFailedEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		failedMessage := api.EventMessage_Failed{
			Failed: &failed,
		}

		message := api.EventMessage{
			Events: &failedMessage,
		}

		return &message, nil

	case v1.PodSucceeded:
		succeeded := api.JobSucceededEvent{
			JobId:    pod.Labels[domain.JobId],
			JobSetId: pod.Labels[domain.JobSetId],
			Queue:    pod.Labels[domain.Queue],
			Created:  time.Now(),
		}

		succeededMessage := api.EventMessage_Succeeded{
			Succeeded: &succeeded,
		}

		message := api.EventMessage{
			Events: &succeededMessage,
		}

		return &message, nil

	default:
		return new(api.EventMessage), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}
