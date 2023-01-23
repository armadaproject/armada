package reporter

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pkg/errors"

	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/api"
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

func CreateJobIngressInfoEvent(pod *v1.Pod, clusterId string, associatedServices []*v1.Service, associatedIngresses []*networking.Ingress) (api.Event, error) {
	if pod.Spec.NodeName == "" || pod.Status.HostIP == "" {
		return nil, errors.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), as pod is not allocated to a node", pod.Name, pod.Namespace)
	}
	if associatedServices == nil || associatedIngresses == nil {
		return nil, errors.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), associated ingresses may not be nil", pod.Name, pod.Namespace)
	}
	if len(associatedServices) == 0 && len(associatedIngresses) == 0 {
		return nil, errors.Errorf("unable to create JobIngressInfoEvent for pod %s (%s), as no associated ingress provided", pod.Name, pod.Namespace)
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
			portNumber := rule.HTTP.Paths[0].Backend.Service.Port.Number
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

func CreateJobPreemptedEvent(clusterEvent *v1.Event, clusterId string) (event *api.JobPreemptedEvent, err error) {
	eventTime := clusterEvent.LastTimestamp.Time
	if eventTime.IsZero() {
		eventTime = time.Now()
	}
	event = &api.JobPreemptedEvent{
		Created:   eventTime,
		ClusterId: clusterId,
	}

	if err := enrichPreemptedEventFromInvolvedObject(event, clusterEvent.InvolvedObject); err != nil {
		return nil, err
	}

	if clusterEvent.Related != nil {
		if err := enrichPreemptedEventFromRelatedObject(event, clusterEvent.Related); err != nil {
			return nil, err
		}
	}

	if err := enrichPreemptedEventFromClusterEventMessage(event, clusterEvent.Message); err != nil {
		return nil, err
	}

	return event, nil
}

func enrichPreemptedEventFromInvolvedObject(event *api.JobPreemptedEvent, involved v1.ObjectReference) error {
	preemptedJobId, err := util.ExtractJobIdFromName(involved.Name)
	if err != nil {
		return errors.WithMessage(err, "error extracting preempted job id from pod name")
	}

	event.JobId = preemptedJobId
	event.RunId = string(involved.UID)

	return nil
}

func enrichPreemptedEventFromRelatedObject(event *api.JobPreemptedEvent, related *v1.ObjectReference) error {
	if util.IsArmadaJobPod(related.Name) {
		preemptiveJobId, err := util.ExtractJobIdFromName(related.Name)
		if err != nil {
			return errors.WithMessage(err, "error extracting preemptive job id from pod name")
		}

		event.PreemptiveJobId = preemptiveJobId
	}

	event.PreemptiveRunId = string(related.UID)

	return nil
}

func enrichPreemptedEventFromClusterEventMessage(event *api.JobPreemptedEvent, msg string) error {
	info, err := util.ParsePreemptionMessage(msg)
	if err != nil {
		return nil
	}

	if !util.IsArmadaJobPod(info.Name) {
		return nil
	}

	if event.PreemptiveJobId == "" {
		preemptiveJobId, err := util.ExtractJobIdFromName(info.Name)
		if err != nil {
			return errors.WithMessage(err, "error extracting preemptive job id from pod name")
		}

		event.PreemptiveJobId = preemptiveJobId
	}

	return nil
}

func CreateSimpleJobFailedEvent(pod *v1.Pod, reason string, clusterId string, cause api.Cause) api.Event {
	return CreateJobFailedEvent(pod, reason, cause, []*api.ContainerStatus{}, map[string]int32{}, clusterId)
}

func CreateJobFailedEvent(pod *v1.Pod, reason string, cause api.Cause, containerStatuses []*api.ContainerStatus,
	exitCodes map[string]int32, clusterId string,
) api.Event {
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

func CreateReturnLeaseEvent(pod *v1.Pod, reason string, clusterId string) api.Event {
	return &api.JobLeaseReturnedEvent{
		JobId:        pod.Labels[domain.JobId],
		JobSetId:     pod.Annotations[domain.JobSetId],
		Queue:        pod.Labels[domain.Queue],
		Created:      time.Now(),
		Reason:       reason,
		ClusterId:    clusterId,
		KubernetesId: string(pod.ObjectMeta.UID),
		PodNumber:    getPodNumber(pod),
	}
}

func CreateJobUtilisationEvent(pod *v1.Pod, utilisationData *domain.UtilisationData, clusterId string) api.Event {
	return &api.JobUtilisationEvent{
		JobId:                 pod.Labels[domain.JobId],
		JobSetId:              pod.Annotations[domain.JobSetId],
		Queue:                 pod.Labels[domain.Queue],
		Created:               time.Now(),
		ClusterId:             clusterId,
		MaxResourcesForPeriod: utilisationData.CurrentUsage,
		TotalCumulativeUsage:  utilisationData.CumulativeUsage,
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
