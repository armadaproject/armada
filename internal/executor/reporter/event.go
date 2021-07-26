package reporter

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1beta1"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/executor/domain"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
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
			PodName:      pod.Name,
			PodNamespace: pod.Namespace,
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
			PodName:      pod.Name,
			PodNamespace: pod.Namespace,
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
			PodName:      pod.Name,
			PodNamespace: pod.Namespace,
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
		PodName:      pod.Name,
		PodNamespace: pod.Namespace,
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

func CreateJobIngressInfoEvent(pod *v1.Pod, clusterId string, associatedServices []*v1.Service, associatedIngresses []*networking.Ingress) (api.Event, error) {
	if pod.Spec.NodeName == "" || pod.Status.HostIP == "" {
		return nil, fmt.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), as pod is not allocated to a node", pod.Name, pod.Namespace)
	}
	if associatedServices == nil || associatedIngresses == nil {
		return nil, fmt.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), associated ingresses may not be nil", pod.Name, pod.Namespace)
	}
	if len(associatedServices) == 0 && len(associatedIngresses) == 0 {
		return nil, fmt.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), as no associated ingress provided", pod.Name, pod.Namespace)
	}
	containerPortMapping := map[int32]string{}
	for _, service := range associatedServices {
		if service.Spec.Type != v1.ServiceTypeNodePort {
			continue
		}
		for _, servicePort := range service.Spec.Ports {
			externalAddress := fmt.Sprintf("%s:%d", pod.Status.HostIP, servicePort.NodePort)
			containerPortMapping[servicePort.Port] = externalAddress
		}
	}

	for _, ingress := range associatedIngresses {
		for _, rule := range ingress.Spec.Rules {
			portNumber := rule.HTTP.Paths[0].Backend.ServicePort.IntVal
			containerPortMapping[portNumber] = rule.Host
		}
	}

	return &api.JobIngressInfoEvent{
		JobId:            pod.Labels[domain.JobId],
		JobSetId:         pod.Annotations[domain.JobSetId],
		Queue:            pod.Labels[domain.Queue],
		Created:          time.Now(),
		ClusterId:        clusterId,
		KubernetesId:     string(pod.ObjectMeta.UID),
		PodNumber:        getPodNumber(pod),
		PodName:          pod.Name,
		PodNamespace:     pod.Namespace,
		NodeName:         pod.Spec.NodeName,
		IngressAddresses: containerPortMapping,
	}, nil
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
		PodName:           pod.Name,
		PodNamespace:      pod.Namespace,
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
		PodName:               pod.Name,
		PodNamespace:          pod.Namespace,
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
		PodName:      pod.Name,
		PodNamespace: pod.Namespace,
		Reason:       reason,
	}
}
