package apimessages

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// FromEventSequence Converts internal messages to external api messages
// Note that some internal api messages can result in multiple api messages so we need to
// return an array of messages
// TODO: once we no longer need to worry about legacy messages in the api we can move eventutil.ApiJobFromLogSubmitJob here
func FromEventSequence(es *armadaevents.EventSequence) ([]*api.EventMessage, error) {
	apiEvents := make([]*api.EventMessage, 0, len(es.Events))
	var err error = nil

	for _, event := range es.Events {
		var convertedEvents []*api.EventMessage = nil
		switch esEvent := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			convertedEvents, err = FromInternalSubmit(es.UserId, es.Groups, es.Queue, es.JobSetName, *event.Created, esEvent.SubmitJob)
		case *armadaevents.EventSequence_Event_CancelledJob:
			convertedEvents, err = FromInternalCancelled(es.UserId, es.Queue, es.JobSetName, *event.Created, esEvent.CancelledJob)
		case *armadaevents.EventSequence_Event_CancelJob:
			convertedEvents, err = FromInternalCancel(es.UserId, es.Queue, es.JobSetName, *event.Created, esEvent.CancelJob)
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			convertedEvents, err = FromInternalReprioritiseJob(es.UserId, es.Queue, es.JobSetName, *event.Created, esEvent.ReprioritiseJob)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			convertedEvents, err = FromInternalReprioritisedJob(es.UserId, es.Queue, es.JobSetName, *event.Created, esEvent.ReprioritisedJob)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			convertedEvents, err = FromInternalLogDuplicateDetected(es.Queue, es.JobSetName, *event.Created, esEvent.JobDuplicateDetected)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			convertedEvents, err = FromInternalLogJobRunLeased(es.Queue, es.JobSetName, *event.Created, esEvent.JobRunLeased)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			convertedEvents, err = FromInternalJobRunErrors(es.Queue, es.JobSetName, *event.Created, esEvent.JobRunErrors)
		case *armadaevents.EventSequence_Event_JobSucceeded:
			convertedEvents, err = FromInternalJobSucceeded(es.Queue, es.JobSetName, *event.Created, esEvent.JobSucceeded)
		case *armadaevents.EventSequence_Event_JobErrors:
			convertedEvents, err = FromInternalJobErrors(es.Queue, es.JobSetName, *event.Created, esEvent.JobErrors)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			convertedEvents, err = FromInternalJobRunRunning(es.Queue, es.JobSetName, *event.Created, esEvent.JobRunRunning)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			convertedEvents, err = FromInternalJobRunAssigned(es.Queue, es.JobSetName, *event.Created, esEvent.JobRunAssigned)
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
			convertedEvents, err = FromInternalResourceUtilisation(es.Queue, es.JobSetName, *event.Created, esEvent.ResourceUtilisation)
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
			convertedEvents, err = FromInternalStandaloneIngressInfo(es.Queue, es.JobSetName, *event.Created, esEvent.StandaloneIngressInfo)
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			convertedEvents, err = FromInternalJobRunPreempted(es.Queue, es.JobSetName, *event.Created, esEvent.JobRunPreempted)
		case *armadaevents.EventSequence_Event_ReprioritiseJobSet,
			*armadaevents.EventSequence_Event_CancelJobSet,
			*armadaevents.EventSequence_Event_JobRunSucceeded,
			*armadaevents.EventSequence_Event_JobRequeued,
			*armadaevents.EventSequence_Event_PartitionMarker:
			// These events have no api analog right now, so we ignore
			log.Debugf("ignoring event")
		default:
			log.Warnf("unknown event type: %T", esEvent)
			convertedEvents = nil
		}
		if err != nil {
			// TODO: would it be better to log a warning and continue- that way one bad event won't cause the stream to terminate?
			return nil, err
		}
		apiEvents = append(apiEvents, convertedEvents...)
	}
	return apiEvents, nil
}

func FromInternalSubmit(owner string, groups []string, queue string, jobSet string, time time.Time, e *armadaevents.SubmitJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	job, err := eventutil.ApiJobFromLogSubmitJob(owner, groups, queue, jobSet, time, e)
	if err != nil {
		return nil, err
	}

	submitEvent := &api.JobSubmittedEvent{
		JobId:    jobId,
		JobSetId: jobSet,
		Queue:    queue,
		Created:  time,
		Job:      *job,
	}

	queuedEvent := &api.JobQueuedEvent{
		JobId:    jobId,
		JobSetId: jobSet,
		Queue:    queue,
		Created:  time,
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Submitted{
				Submitted: submitEvent,
			},
		},
		{
			Events: &api.EventMessage_Queued{
				Queued: queuedEvent,
			},
		},
	}, nil
}

func FromInternalCancel(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.CancelJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelling{
				Cancelling: &api.JobCancellingEvent{
					JobId:     jobId,
					JobSetId:  jobSetName,
					Queue:     queueName,
					Created:   time,
					Requestor: userId,
				},
			},
		},
	}, nil
}

func FromInternalCancelled(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.CancelledJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Cancelled{
				Cancelled: &api.JobCancelledEvent{
					JobId:     jobId,
					JobSetId:  jobSetName,
					Queue:     queueName,
					Created:   time,
					Requestor: userId,
				},
			},
		},
	}, nil
}

func FromInternalReprioritiseJob(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.ReprioritiseJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}
	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritizing{
				Reprioritizing: &api.JobReprioritizingEvent{
					JobId:       jobId,
					JobSetId:    jobSetName,
					Queue:       queueName,
					Created:     time,
					NewPriority: float64(e.Priority),
					Requestor:   userId,
				},
			},
		},
	}, nil
}

func FromInternalReprioritisedJob(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.ReprioritisedJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}
	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Reprioritized{
				Reprioritized: &api.JobReprioritizedEvent{
					JobId:       jobId,
					JobSetId:    jobSetName,
					Queue:       queueName,
					Created:     time,
					NewPriority: float64(e.Priority),
					Requestor:   userId,
				},
			},
		},
	}, nil
}

func FromInternalLogDuplicateDetected(queueName string, jobSetName string, time time.Time, e *armadaevents.JobDuplicateDetected) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.NewJobId)
	if err != nil {
		return nil, err
	}
	originalJobId, err := armadaevents.UlidStringFromProtoUuid(e.OldJobId)
	if err != nil {
		return nil, err
	}
	return []*api.EventMessage{
		{
			Events: &api.EventMessage_DuplicateFound{
				DuplicateFound: &api.JobDuplicateFoundEvent{
					JobId:         jobId,
					JobSetId:      jobSetName,
					Queue:         queueName,
					Created:       time,
					OriginalJobId: originalJobId,
				},
			},
		},
	}, nil
}

func FromInternalLogJobRunLeased(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunLeased) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}
	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Leased{
				Leased: &api.JobLeasedEvent{
					JobId:     jobId,
					JobSetId:  jobSetName,
					Queue:     queueName,
					Created:   time,
					ClusterId: e.ExecutorId,
				},
			},
		},
	}, nil
}

func FromInternalJobSucceeded(queueName string, jobSetName string, time time.Time, e *armadaevents.JobSucceeded) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	apiEvent := &api.JobSucceededEvent{
		JobId:    jobId,
		JobSetId: jobSetName,
		Queue:    queueName,
		Created:  time,
	}

	if len(e.ResourceInfos) > 0 {
		ri := e.ResourceInfos[0]
		apiEvent.ClusterId = ri.GetObjectMeta().GetExecutorId()
		apiEvent.PodNamespace = ri.GetObjectMeta().GetNamespace()
		apiEvent.PodName = ri.GetObjectMeta().GetName()
		apiEvent.KubernetesId = ri.GetObjectMeta().GetKubernetesId()
		apiEvent.NodeName = ri.GetPodInfo().GetNodeName()
		apiEvent.PodNumber = ri.GetPodInfo().GetPodNumber()
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Succeeded{
				Succeeded: apiEvent,
			},
		},
	}, nil
}

func FromInternalJobRunErrors(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunErrors) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	events := make([]*api.EventMessage, 0)
	for _, msgErr := range e.GetErrors() {
		switch reason := msgErr.Reason.(type) {
		case *armadaevents.Error_LeaseExpired:
			event := &api.EventMessage{
				Events: &api.EventMessage_LeaseExpired{
					LeaseExpired: &api.JobLeaseExpiredEvent{
						JobId:    jobId,
						JobSetId: jobSetName,
						Queue:    queueName,
						Created:  time,
					},
				},
			}
			events = append(events, event)
		case *armadaevents.Error_PodUnschedulable:
			objectMeta := reason.PodUnschedulable.GetObjectMeta()
			event := &api.EventMessage{
				Events: &api.EventMessage_UnableToSchedule{
					UnableToSchedule: &api.JobUnableToScheduleEvent{
						JobId:        jobId,
						ClusterId:    objectMeta.GetExecutorId(),
						PodNamespace: objectMeta.GetNamespace(),
						PodName:      objectMeta.GetName(),
						KubernetesId: objectMeta.GetKubernetesId(),
						Reason:       reason.PodUnschedulable.GetMessage(),
						NodeName:     reason.PodUnschedulable.GetNodeName(),
						PodNumber:    reason.PodUnschedulable.GetPodNumber(),
						JobSetId:     jobSetName,
						Queue:        queueName,
						Created:      time,
					},
				},
			}
			events = append(events, event)
		case *armadaevents.Error_PodLeaseReturned:
			objectMeta := reason.PodLeaseReturned.GetObjectMeta()
			event := &api.EventMessage{
				Events: &api.EventMessage_LeaseReturned{
					LeaseReturned: &api.JobLeaseReturnedEvent{
						JobId:        jobId,
						JobSetId:     jobSetName,
						Queue:        queueName,
						Created:      time,
						ClusterId:    objectMeta.GetExecutorId(),
						Reason:       reason.PodLeaseReturned.GetMessage(),
						KubernetesId: objectMeta.GetKubernetesId(),
						PodNumber:    reason.PodLeaseReturned.GetPodNumber(),
						RunAttempted: reason.PodLeaseReturned.GetRunAttempted(),
					},
				},
			}
			events = append(events, event)
		case *armadaevents.Error_PodTerminated:
			objectMeta := reason.PodTerminated.GetObjectMeta()
			event := &api.EventMessage{
				Events: &api.EventMessage_Terminated{
					Terminated: &api.JobTerminatedEvent{
						JobId:        jobId,
						JobSetId:     jobSetName,
						PodNamespace: objectMeta.GetNamespace(),
						PodName:      objectMeta.GetName(),
						Queue:        queueName,
						Created:      time,
						ClusterId:    objectMeta.GetExecutorId(),
						Reason:       reason.PodTerminated.GetMessage(),
						KubernetesId: objectMeta.GetKubernetesId(),
						PodNumber:    reason.PodTerminated.GetPodNumber(),
					},
				},
			}
			events = append(events, event)
		default:
			log.Debugf("Ignoring event %T", reason)
		}
	}
	return events, nil
}

func FromInternalJobErrors(queueName string, jobSetName string, time time.Time, e *armadaevents.JobErrors) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}
	events := make([]*api.EventMessage, 0)
	for _, msgErr := range e.GetErrors() {
		if !msgErr.Terminal {
			continue
		}
		switch reason := msgErr.Reason.(type) {
		case *armadaevents.Error_PodError:
			event := &api.EventMessage{
				Events: &api.EventMessage_Failed{
					Failed: makeJobFailed(jobId, queueName, jobSetName, time, reason),
				},
			}
			events = append(events, event)
		case *armadaevents.Error_JobRunPreemptedError:
			event := &api.EventMessage{
				Events: &api.EventMessage_Failed{
					Failed: &api.JobFailedEvent{
						JobId:    jobId,
						JobSetId: jobSetName,
						Queue:    queueName,
						Created:  time,
						Reason:   "preempted",
					},
				},
			}
			events = append(events, event)
		case *armadaevents.Error_MaxRunsExceeded:
			event := &api.EventMessage{
				Events: &api.EventMessage_Failed{
					Failed: &api.JobFailedEvent{
						JobId:    jobId,
						JobSetId: jobSetName,
						Queue:    queueName,
						Created:  time,
						Reason:   reason.MaxRunsExceeded.Message,
					},
				},
			}
			events = append(events, event)
		default:
			log.Warnf("unknown error %T for job %s", reason, jobId)
			event := &api.EventMessage{
				Events: &api.EventMessage_Failed{
					Failed: &api.JobFailedEvent{
						JobId:    jobId,
						JobSetId: jobSetName,
						Queue:    queueName,
						Created:  time,
					},
				},
			}
			events = append(events, event)
		}
	}
	return events, nil
}

func FromInternalJobRunRunning(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunRunning) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	apiEvent := &api.JobRunningEvent{
		JobId:    jobId,
		JobSetId: jobSetName,
		Queue:    queueName,
		Created:  time,
	}

	if len(e.ResourceInfos) > 0 {
		ri := e.ResourceInfos[0]
		apiEvent.ClusterId = ri.GetObjectMeta().GetExecutorId()
		apiEvent.PodNamespace = ri.GetObjectMeta().GetNamespace()
		apiEvent.PodName = ri.GetObjectMeta().GetName()
		apiEvent.KubernetesId = ri.GetObjectMeta().GetKubernetesId()
		apiEvent.NodeName = ri.GetPodInfo().GetNodeName()
		apiEvent.PodNumber = ri.GetPodInfo().GetPodNumber()
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Running{
				Running: apiEvent,
			},
		},
	}, nil
}

func FromInternalJobRunAssigned(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunAssigned) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	apiEvent := &api.JobPendingEvent{
		JobId:    jobId,
		JobSetId: jobSetName,
		Queue:    queueName,
		Created:  time,
	}

	if len(e.ResourceInfos) > 0 {
		ri := e.ResourceInfos[0]
		apiEvent.ClusterId = ri.GetObjectMeta().GetExecutorId()
		apiEvent.PodNamespace = ri.GetObjectMeta().GetNamespace()
		apiEvent.PodName = ri.GetObjectMeta().GetName()
		apiEvent.KubernetesId = ri.GetObjectMeta().GetKubernetesId()
		apiEvent.PodNumber = ri.GetPodInfo().GetPodNumber()
	}
	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Pending{
				Pending: apiEvent,
			},
		},
	}, nil
}

func FromInternalJobRunPreempted(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunPreempted) ([]*api.EventMessage, error) {
	if e == nil {
		// We only support PodPreempted right now
		return nil, nil
	}

	jobId, err := armadaevents.UlidStringFromProtoUuid(e.PreemptedJobId)
	if err != nil {
		return nil, err
	}
	runId, err := armadaevents.UuidStringFromProtoUuid(e.PreemptedRunId)
	if err != nil {
		return nil, err
	}

	preemptiveJobId := ""
	preemptiveRunId := ""

	if e.PreemptiveJobId != nil {
		preemptiveJobId, err = armadaevents.UlidStringFromProtoUuid(e.PreemptiveJobId)
		if err != nil {
			return nil, err
		}
	}
	if e.PreemptiveRunId != nil {
		preemptiveRunId, err = armadaevents.UuidStringFromProtoUuid(e.PreemptiveRunId)
		if err != nil {
			return nil, err
		}
	}

	apiEvent := &api.JobPreemptedEvent{
		JobId:           jobId,
		JobSetId:        jobSetName,
		Queue:           queueName,
		Created:         time,
		RunId:           runId,
		PreemptiveJobId: preemptiveJobId,
		PreemptiveRunId: preemptiveRunId,
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Preempted{
				Preempted: apiEvent,
			},
		},
	}, nil
}

func FromInternalResourceUtilisation(queueName string, jobSetName string, time time.Time, e *armadaevents.ResourceUtilisation) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	apiEvent := &api.JobUtilisationEvent{
		JobId:                 jobId,
		JobSetId:              jobSetName,
		Queue:                 queueName,
		Created:               time,
		ClusterId:             e.GetResourceInfo().GetObjectMeta().GetExecutorId(),
		KubernetesId:          e.GetResourceInfo().GetObjectMeta().GetKubernetesId(),
		MaxResourcesForPeriod: e.MaxResourcesForPeriod,
		NodeName:              e.GetResourceInfo().GetPodInfo().GetNodeName(),
		PodNumber:             e.GetResourceInfo().GetPodInfo().GetPodNumber(),
		PodName:               e.GetResourceInfo().GetObjectMeta().GetName(),
		PodNamespace:          e.GetResourceInfo().GetObjectMeta().GetNamespace(),
		TotalCumulativeUsage:  e.TotalCumulativeUsage,
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Utilisation{
				Utilisation: apiEvent,
			},
		},
	}, nil
}

func FromInternalStandaloneIngressInfo(queueName string, jobSetName string, time time.Time, e *armadaevents.StandaloneIngressInfo) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		return nil, err
	}

	apiEvent := &api.JobIngressInfoEvent{
		JobId:            jobId,
		JobSetId:         jobSetName,
		Queue:            queueName,
		Created:          time,
		ClusterId:        e.GetObjectMeta().GetExecutorId(),
		KubernetesId:     e.GetObjectMeta().GetKubernetesId(),
		NodeName:         e.GetNodeName(),
		PodNumber:        e.GetPodNumber(),
		PodName:          e.GetPodName(),
		PodNamespace:     e.GetObjectMeta().GetNamespace(),
		IngressAddresses: e.GetIngressAddresses(),
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_IngressInfo{
				IngressInfo: apiEvent,
			},
		},
	}, nil
}

func makeJobFailed(jobId string, queueName string, jobSetName string, time time.Time, podErrorEvent *armadaevents.Error_PodError) *api.JobFailedEvent {
	podError := podErrorEvent.PodError
	event := &api.JobFailedEvent{
		JobId:        jobId,
		JobSetId:     jobSetName,
		Queue:        queueName,
		Created:      time,
		ClusterId:    podError.GetObjectMeta().GetExecutorId(),
		PodNamespace: podError.GetObjectMeta().GetNamespace(),
		KubernetesId: podError.GetObjectMeta().GetKubernetesId(),
		PodNumber:    podError.GetPodNumber(),
		NodeName:     podError.GetNodeName(),
		Reason:       podError.GetMessage(),
		PodName:      podError.GetObjectMeta().GetName(),
	}
	switch podError.KubernetesReason {
	case armadaevents.KubernetesReason_DeadlineExceeded:
		event.Cause = api.Cause_DeadlineExceeded
	case armadaevents.KubernetesReason_AppError:
		event.Cause = api.Cause_Error
	case armadaevents.KubernetesReason_Evicted:
		event.Cause = api.Cause_Evicted
	case armadaevents.KubernetesReason_OOM:
		event.Cause = api.Cause_OOM
	default:
		log.Warnf("Unknown KubernetesReason of type %T", podError.KubernetesReason)
	}

	containerStatuses := make([]*api.ContainerStatus, 0)
	for _, containerErr := range podError.ContainerErrors {
		containerStatus := &api.ContainerStatus{
			Name:     containerErr.GetObjectMeta().GetName(),
			ExitCode: containerErr.GetExitCode(),
			Message:  containerErr.Message,
			Reason:   containerErr.Reason,
		}
		switch containerErr.KubernetesReason {
		case armadaevents.KubernetesReason_DeadlineExceeded:
			containerStatus.Cause = api.Cause_DeadlineExceeded
		case armadaevents.KubernetesReason_AppError:
			containerStatus.Cause = api.Cause_Error
		case armadaevents.KubernetesReason_Evicted:
			containerStatus.Cause = api.Cause_Evicted
		case armadaevents.KubernetesReason_OOM:
			containerStatus.Cause = api.Cause_OOM
		default:
			log.Warnf("Unknown KubernetesReason of type %T", containerErr.KubernetesReason)
		}
		containerStatuses = append(containerStatuses, containerStatus)
	}
	event.ContainerStatuses = containerStatuses
	return event
}
