package reporter

import (
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/types"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	networking "k8s.io/api/networking/v1"

	"github.com/armadaproject/armada/internal/executor/domain"
	"github.com/armadaproject/armada/internal/executor/util"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func CreateEventForCurrentState(pod *v1.Pod, clusterId string) (*armadaevents.EventSequence, error) {
	phase := pod.Status.Phase
	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}
	now := types.TimestampNow()

	switch phase {
	case v1.PodPending:
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: now,
			Event: &armadaevents.EventSequence_Event_JobRunAssigned{
				JobRunAssigned: &armadaevents.JobRunAssigned{
					RunId:    runId,
					RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
					JobId:    jobId,
					JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								KubernetesId: string(pod.ObjectMeta.UID),
								Name:         pod.Name,
								Namespace:    pod.Namespace,
								ExecutorId:   clusterId,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									PodNumber: getPodNumber(pod),
								},
							},
						},
					},
				},
			},
		})
		return sequence, nil
	case v1.PodRunning:
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: now,
			Event: &armadaevents.EventSequence_Event_JobRunRunning{
				JobRunRunning: &armadaevents.JobRunRunning{
					RunId:    runId,
					RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
					JobId:    jobId,
					JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								KubernetesId: string(pod.ObjectMeta.UID),
								Name:         pod.Name,
								Namespace:    pod.Namespace,
								ExecutorId:   clusterId,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									NodeName:  pod.Spec.NodeName,
									PodNumber: getPodNumber(pod),
								},
							},
						},
					},
				},
			},
		})
		return sequence, nil
	case v1.PodFailed:
		return CreateJobFailedEvent(
			pod,
			util.ExtractPodFailedReason(pod),
			util.ExtractPodFailureCause(pod),
			"",
			util.ExtractFailedPodContainerStatuses(pod, clusterId),
			clusterId)
	case v1.PodSucceeded:
		sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
			Created: now,
			Event: &armadaevents.EventSequence_Event_JobRunSucceeded{
				JobRunSucceeded: &armadaevents.JobRunSucceeded{
					RunId:    runId,
					RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
					JobId:    jobId,
					JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
					ResourceInfos: []*armadaevents.KubernetesResourceInfo{
						{
							ObjectMeta: &armadaevents.ObjectMeta{
								KubernetesId: string(pod.ObjectMeta.UID),
								Name:         pod.Name,
								Namespace:    pod.Namespace,
								ExecutorId:   clusterId,
							},
							Info: &armadaevents.KubernetesResourceInfo_PodInfo{
								PodInfo: &armadaevents.PodInfo{
									NodeName:  pod.Spec.NodeName,
									PodNumber: getPodNumber(pod),
								},
							},
						},
					},
				},
			},
		})
		return sequence, nil
	default:
		return nil, errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
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

func CreateJobUnableToScheduleEvent(pod *v1.Pod, reason string, clusterId string) (*armadaevents.EventSequence, error) {
	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId:    runId,
				RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
				JobId:    jobId,
				JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				Errors: []*armadaevents.Error{
					{
						Terminal: false, // EventMessage_UnableToSchedule indicates an issue with job to start up - info only
						Reason: &armadaevents.Error_PodUnschedulable{
							PodUnschedulable: &armadaevents.PodUnschedulable{
								ObjectMeta: &armadaevents.ObjectMeta{
									KubernetesId: string(pod.ObjectMeta.UID),
									Name:         pod.Name,
									Namespace:    pod.Namespace,
									ExecutorId:   clusterId,
								},
								Message:   reason,
								NodeName:  pod.Spec.NodeName,
								PodNumber: getPodNumber(pod),
							},
						},
					},
				},
			},
		},
	})

	return sequence, nil
}

func CreateJobIngressInfoEvent(pod *v1.Pod, clusterId string, associatedServices []*v1.Service, associatedIngresses []*networking.Ingress) (*armadaevents.EventSequence, error) {
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

	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_StandaloneIngressInfo{
			StandaloneIngressInfo: &armadaevents.StandaloneIngressInfo{
				RunId:    runId,
				RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
				JobId:    jobId,
				JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				ObjectMeta: &armadaevents.ObjectMeta{
					KubernetesId: string(pod.ObjectMeta.UID),
					Namespace:    pod.Namespace,
					ExecutorId:   clusterId,
				},
				IngressAddresses: containerPortMapping,
				NodeName:         pod.Spec.NodeName,
				PodName:          pod.Name,
				PodNumber:        getPodNumber(pod),
				PodNamespace:     pod.Namespace,
			},
		},
	})
	return sequence, nil
}

func CreateSimpleJobPreemptedEvent(pod *v1.Pod) (*armadaevents.EventSequence, error) {
	sequence := createEmptySequence(pod)
	preemptedJobId, preemptedRunId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_JobRunPreempted{
			JobRunPreempted: &armadaevents.JobRunPreempted{
				PreemptedJobId:    preemptedJobId,
				PreemptedJobIdStr: armadaevents.MustUlidStringFromProtoUuid(preemptedJobId),
				PreemptedRunId:    preemptedRunId,
				PreemptedRunIdStr: armadaevents.MustUuidStringFromProtoUuid(preemptedRunId),
			},
		},
	})
	return sequence, nil
}

func CreateSimpleJobFailedEvent(pod *v1.Pod, reason string, debugMessage string, clusterId string, cause armadaevents.KubernetesReason) (*armadaevents.EventSequence, error) {
	return CreateJobFailedEvent(pod, reason, cause, debugMessage, []*armadaevents.ContainerError{}, clusterId)
}

func CreateJobFailedEvent(pod *v1.Pod, reason string, cause armadaevents.KubernetesReason, debugMessage string,
	containerStatuses []*armadaevents.ContainerError, clusterId string,
) (*armadaevents.EventSequence, error) {
	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId:    runId,
				RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
				JobId:    jobId,
				JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								ObjectMeta: &armadaevents.ObjectMeta{
									KubernetesId: string(pod.ObjectMeta.UID),
									Namespace:    pod.Namespace,
									ExecutorId:   clusterId,
									Name:         pod.Name,
								},
								Message:          reason,
								NodeName:         pod.Spec.NodeName,
								PodNumber:        getPodNumber(pod),
								ContainerErrors:  containerStatuses,
								KubernetesReason: cause,
								DebugMessage:     debugMessage,
							},
						},
					},
				},
			},
		},
	})
	return sequence, nil
}

func CreateMinimalJobFailedEvent(jobIdStr string, runIdStr string, jobSet string, queue string, clusterId string, message string) (*armadaevents.EventSequence, error) {
	sequence := &armadaevents.EventSequence{}
	sequence.Queue = queue
	sequence.JobSetName = jobSet

	jobId, err := armadaevents.ProtoUuidFromUlidString(jobIdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert jobId %s to uuid - %s", jobIdStr, err)
	}

	runId, err := armadaevents.ProtoUuidFromUuidString(runIdStr)
	if err != nil {
		return nil, fmt.Errorf("failed to convert runId %s to uuid - %s", runIdStr, err)
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId:    runId,
				RunIdStr: runIdStr,
				JobId:    jobId,
				JobIdStr: jobIdStr,
				Errors: []*armadaevents.Error{
					{
						Terminal: true,
						Reason: &armadaevents.Error_PodError{
							PodError: &armadaevents.PodError{
								ObjectMeta: &armadaevents.ObjectMeta{
									ExecutorId: clusterId,
								},
								Message:          message,
								ContainerErrors:  []*armadaevents.ContainerError{},
								KubernetesReason: armadaevents.KubernetesReason_AppError,
							},
						},
					},
				},
			},
		},
	})

	return sequence, nil
}

func CreateReturnLeaseEvent(pod *v1.Pod, reason string, debugMessage string, clusterId string, runAttempted bool) (*armadaevents.EventSequence, error) {
	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_JobRunErrors{
			JobRunErrors: &armadaevents.JobRunErrors{
				RunId:    runId,
				RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
				JobId:    jobId,
				JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				Errors: []*armadaevents.Error{
					{
						Terminal: true, // EventMessage_LeaseReturned indicates a pod could not be scheduled.
						Reason: &armadaevents.Error_PodLeaseReturned{
							PodLeaseReturned: &armadaevents.PodLeaseReturned{
								ObjectMeta: &armadaevents.ObjectMeta{
									KubernetesId: string(pod.ObjectMeta.UID),
									Name:         pod.Name,
									Namespace:    pod.Namespace,
									ExecutorId:   clusterId,
								},
								PodNumber:    getPodNumber(pod),
								Message:      reason,
								RunAttempted: runAttempted,
								DebugMessage: debugMessage,
							},
						},
					},
				},
			},
		},
	})
	return sequence, nil
}

func CreateJobUtilisationEvent(pod *v1.Pod, utilisationData *domain.UtilisationData, clusterId string) (*armadaevents.EventSequence, error) {
	sequence := createEmptySequence(pod)
	jobId, runId, err := extractIds(pod)
	if err != nil {
		return nil, err
	}

	sequence.Events = append(sequence.Events, &armadaevents.EventSequence_Event{
		Created: types.TimestampNow(),
		Event: &armadaevents.EventSequence_Event_ResourceUtilisation{
			ResourceUtilisation: &armadaevents.ResourceUtilisation{
				RunId:    runId,
				RunIdStr: armadaevents.MustUuidStringFromProtoUuid(runId),
				JobId:    jobId,
				JobIdStr: armadaevents.MustUlidStringFromProtoUuid(jobId),
				ResourceInfo: &armadaevents.KubernetesResourceInfo{
					ObjectMeta: &armadaevents.ObjectMeta{
						KubernetesId: string(pod.ObjectMeta.UID),
						Name:         pod.Name,
						Namespace:    pod.Namespace,
						ExecutorId:   clusterId,
					},
					Info: &armadaevents.KubernetesResourceInfo_PodInfo{
						PodInfo: &armadaevents.PodInfo{
							NodeName:  pod.Spec.NodeName,
							PodNumber: getPodNumber(pod),
						},
					},
				},
				MaxResourcesForPeriod: utilisationData.CurrentUsage.ToProtoMap(),
				TotalCumulativeUsage:  utilisationData.CumulativeUsage.ToProtoMap(),
			},
		},
	})
	return sequence, nil
}

func createEmptySequence(pod *v1.Pod) *armadaevents.EventSequence {
	sequence := &armadaevents.EventSequence{}
	sequence.Queue = pod.Labels[domain.Queue]
	sequence.JobSetName = pod.Annotations[domain.JobSetId]
	return sequence
}

func extractIds(pod *v1.Pod) (*armadaevents.Uuid, *armadaevents.Uuid, error) {
	jobId, err := armadaevents.ProtoUuidFromUlidString(pod.Labels[domain.JobId])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert jobId %s to uuid - %s", pod.Labels[domain.JobId], err)
	}

	runId, err := armadaevents.ProtoUuidFromUuidString(pod.Labels[domain.JobRunId])
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert runId %s to uuid - %s", pod.Labels[domain.JobRunId], err)
	}

	return jobId, runId, nil
}
