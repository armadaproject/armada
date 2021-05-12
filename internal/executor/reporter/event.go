package reporter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"

	v1 "k8s.io/api/core/v1"
)

func CreateEventForCurrentState(pod *v1.Pod, clusterId string) (api.Event, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		return &api.JobPendingEvent{
			JobId:        pod.Labels[domain.JobId],
			JobSetId:     pod.Annotations[domain.JobSetId],
			Queue:        pod.Labels[domain.Queue],
			Created:      time.Now(),
			ClusterId:    clusterId,
			KubernetesId: string(pod.ObjectMeta.UID),
			PodNumber:    getPodNumber(pod),
		}, nil
	case v1.PodRunning:
		return &api.JobRunningEvent{
			JobId:        pod.Labels[domain.JobId],
			JobSetId:     pod.Annotations[domain.JobSetId],
			Queue:        pod.Labels[domain.Queue],
			Created:      time.Now(),
			ClusterId:    clusterId,
			KubernetesId: string(pod.ObjectMeta.UID),
			PodNumber:    getPodNumber(pod),
			NodeName:     pod.Spec.NodeName,
		}, nil
	case v1.PodFailed:
		return CreateJobFailedEvent(
			pod,
			util.ExtractPodFailedReason(pod),
			util.ExtractPodFailedCause(pod),
			util.ExtractFailedPodContainerStatuses(pod),
			util.ExtractPodExitCodes(pod),
			clusterId), nil
	case v1.PodSucceeded:
		return &api.JobSucceededEvent{
			JobId:        pod.Labels[domain.JobId],
			JobSetId:     pod.Annotations[domain.JobSetId],
			Queue:        pod.Labels[domain.Queue],
			Created:      time.Now(),
			ClusterId:    clusterId,
			KubernetesId: string(pod.ObjectMeta.UID),
			PodNumber:    getPodNumber(pod),
			NodeName:     pod.Spec.NodeName,
		}, nil
	default:
		return *new(api.Event), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}

func getPodNumber(pod *v1.Pod) int32 {
	podNumberString, ok := pod.Labels[domain.PodNumber]
	if !ok {
		return 0
	}
	podNumber, _ := strconv.Atoi(podNumberString)
	return int32(podNumber)
}

func CreateJobUnableToScheduleEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobUnableToScheduleEvent{
		JobId:        pod.Labels[domain.JobId],
		JobSetId:     pod.Annotations[domain.JobSetId],
		Queue:        pod.Labels[domain.Queue],
		Created:      time.Now(),
		ClusterId:    clusterId,
		Reason:       reason,
		KubernetesId: string(pod.ObjectMeta.UID),
		PodNumber:    getPodNumber(pod),
		NodeName:     pod.Spec.NodeName,
	}
}

func CreateJobLeaseReturnedEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobLeaseReturnedEvent{
		JobId:     pod.Labels[domain.JobId],
		JobSetId:  pod.Annotations[domain.JobSetId],
		Queue:     pod.Labels[domain.Queue],
		Created:   time.Now(),
		ClusterId: clusterId,
		Reason:    reason,
	}
}

func CreateSimpleJobFailedEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return CreateJobFailedEvent(pod, reason, api.Cause_Error, []*api.ContainerStatus{}, map[string]int32{}, clusterId)
}

func CreateJobFailedEvent(pod *v1.Pod, reason string, cause api.Cause, containerStatuses []*api.ContainerStatus,
	exitCodes map[string]int32, clusterId string) api.Event {
	return &api.JobFailedEvent{
		JobId:             pod.Labels[domain.JobId],
		JobSetId:          pod.Annotations[domain.JobSetId],
		Queue:             pod.Labels[domain.Queue],
		Created:           time.Now(),
		ClusterId:         clusterId,
		Reason:            reason,
		ExitCodes:         exitCodes,
		KubernetesId:      string(pod.ObjectMeta.UID),
		PodNumber:         getPodNumber(pod),
		NodeName:          pod.Spec.NodeName,
		ContainerStatuses: containerStatuses,
		Cause:             cause,
	}
}

func CreateJobUtilisationEvent(pod *v1.Pod, maxResources common.ComputeResources, clusterId string) api.Event {
	return &api.JobUtilisationEvent{
		JobId:                 pod.Labels[domain.JobId],
		JobSetId:              pod.Annotations[domain.JobSetId],
		Queue:                 pod.Labels[domain.Queue],
		Created:               time.Now(),
		ClusterId:             clusterId,
		MaxResourcesForPeriod: maxResources,
		KubernetesId:          string(pod.ObjectMeta.UID),
		PodNumber:             getPodNumber(pod),
		NodeName:              pod.Spec.NodeName,
	}
}

func CreateJobTerminatedEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobTerminatedEvent{
		JobId:        pod.Labels[domain.JobId],
		JobSetId:     pod.Annotations[domain.JobSetId],
		Queue:        pod.Labels[domain.Queue],
		Created:      time.Now(),
		ClusterId:    clusterId,
		KubernetesId: string(pod.ObjectMeta.UID),
		PodNumber:    getPodNumber(pod),
		Reason:       reason,
	}
}
