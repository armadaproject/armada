package serving

import (
	"time"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/pkg/errors"
)

func FromEventSequence(es *armadaevents.EventSequence, ts time.Time) ([]*api.EventMessage, error) {
	apiEvents := make([]*api.EventMessage, 0)
	var err error = nil
	var convertedEvents []*api.EventMessage = nil
	for _, event := range es.Events {
		switch esEvent := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			convertedEvents, err = FromLogSubmit(es.Queue, es.JobSetName, ts, esEvent.SubmitJob)
		case *armadaevents.EventSequence_Event_CancelledJob:
			convertedEvents, err = FromLogCancelled(es.UserId, es.Queue, es.JobSetName, ts, esEvent.CancelledJob)
		case *armadaevents.EventSequence_Event_CancelJob:
			convertedEvents, err = FromLogCancelling(es.UserId, es.Queue, es.JobSetName, ts, esEvent.CancelJob)
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			convertedEvents, err = FromLogReprioritizing(es.UserId, es.Queue, es.JobSetName, ts, esEvent.ReprioritiseJob)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			convertedEvents, err = FromLogReprioritised(es.UserId, es.Queue, es.JobSetName, ts, esEvent.ReprioritisedJob)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			convertedEvents, err = FromLogDuplicateDetected(es.Queue, es.JobSetName, ts, esEvent.JobDuplicateDetected)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			convertedEvents, err = FromLogJobRunLeased(es.Queue, es.JobSetName, ts, esEvent.JobRunLeased)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			convertedEvents, err = FromJobRunErrors(es.Queue, es.JobSetName, ts, esEvent.JobRunErrors)
		case *armadaevents.EventSequence_Event_JobErrors:
			convertedEvents, err = FromJobErrors(es.Queue, es.JobSetName, ts, esEvent.JobErrors)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			convertedEvents, err = FromJobRunRunning(es.Queue, es.JobSetName, ts, esEvent.JobRunRunning)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			convertedEvents, err = FromJobRunAssigned(es.Queue, es.JobSetName, ts, esEvent.JobRunAssigned)
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
			convertedEvents, err = FromResourceUtilisation(es.Queue, es.JobSetName, ts, esEvent.ResourceUtilisation)
		}
		if err != nil {
			//TODO: would it be better to log a warning and continue?
			return nil, err
		}
		apiEvents = append(apiEvents, convertedEvents...)
	}
	return apiEvents, nil
}

func FromLogSubmit(queueName string, jobSetName string, time time.Time, e *armadaevents.SubmitJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	return []*api.EventMessage{
		{
			Events: &api.EventMessage_Queued{
				Queued: &api.JobQueuedEvent{
					JobId:    jobId,
					JobSetId: jobSetName,
					Queue:    queueName,
					Created:  time,
				},
			},
		},
	}, nil
}

func FromLogCancelling(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.CancelJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromLogCancelled(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.CancelledJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromLogReprioritizing(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.ReprioritiseJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromLogReprioritised(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.ReprioritisedJob) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromLogDuplicateDetected(queueName string, jobSetName string, time time.Time, e *armadaevents.JobDuplicateDetected) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.NewJobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	originalJobId, err := armadaevents.UlidStringFromProtoUuid(e.OldJobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromLogJobRunLeased(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunLeased) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromJobRunErrors(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunErrors) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	events := make([]*api.EventMessage, 0)
	for _, msgErr := range e.GetErrors() {
		if msgErr.Terminal {
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
			case *armadaevents.Error_PodError:
				event := &api.EventMessage{
					Events: &api.EventMessage_Failed{
						Failed: makeJobFailed(jobId, queueName, jobSetName, time, reason),
					},
				}
				// TODO: determine if we should be making a leaseReturned
				//event := makeLeaseReturned(jobId, queueName, jobSetName, time, reason)
				events = append(events, event)
			}
		}
	}
	return events, nil
}

func makeLeaseReturned(jobId string, queueName string, jobSetName string, time time.Time, podError *armadaevents.Error_PodError) *api.JobLeaseReturnedEvent {
	return &api.JobLeaseReturnedEvent{
		JobId:        jobId,
		JobSetId:     jobSetName,
		Queue:        queueName,
		Created:      time,
		ClusterId:    podError.PodError.GetObjectMeta().GetExecutorId(),
		KubernetesId: podError.PodError.GetObjectMeta().GetKubernetesId(),
		PodNumber:    podError.PodError.GetPodNumber(),
		Reason:       podError.PodError.GetMessage(),
	}
}

func makeJobFailed(jobId string, queueName string, jobSetName string, time time.Time, podError *armadaevents.Error_PodError) *api.JobFailedEvent {
	event := &api.JobFailedEvent{
		JobId:        jobId,
		JobSetId:     jobSetName,
		Queue:        queueName,
		Created:      time,
		ClusterId:    podError.PodError.GetObjectMeta().GetExecutorId(),
		PodNamespace: podError.PodError.GetObjectMeta().GetNamespace(),
		KubernetesId: podError.PodError.GetObjectMeta().GetKubernetesId(),
		PodNumber:    podError.PodError.GetPodNumber(),
		NodeName:     podError.PodError.GetNodeName(),
		Reason:       podError.PodError.GetMessage(),
	}
	containerStatuses := make([]*api.ContainerStatus, 0)
	for _, containerErr := range podError.PodError.ContainerErrors {
		containerStatus := &api.ContainerStatus{
			Name:     containerErr.GetObjectMeta().GetName(),
			ExitCode: containerErr.GetExitCode(),
			Message:  containerErr.Message,
			Reason:   containerErr.Reason,
		}
		switch containerErr.KubernetesReason.(type) {
		case *armadaevents.ContainerError_DeadlineExceeded_:
			containerStatus.Cause = api.Cause_DeadlineExceeded
		case *armadaevents.ContainerError_Error:
			containerStatus.Cause = api.Cause_Error
		case *armadaevents.ContainerError_Evicted_:
			containerStatus.Cause = api.Cause_Evicted
		case *armadaevents.ContainerError_OutOfMemory_:
			containerStatus.Cause = api.Cause_OOM
		}
		containerStatuses = append(containerStatuses, containerStatus)
	}
	event.ContainerStatuses = containerStatuses
	return event
}

func FromJobErrors(queueName string, jobSetName string, time time.Time, e *armadaevents.JobErrors) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	events := make([]*api.EventMessage, 0)
	for _, msgErr := range e.GetErrors() {
		switch reason := msgErr.Reason.(type) {
		case *armadaevents.Error_PodUnschedulable:
			event := &api.EventMessage{
				Events: &api.EventMessage_UnableToSchedule{
					UnableToSchedule: &api.JobUnableToScheduleEvent{
						JobId:        jobId,
						JobSetId:     jobSetName,
						Queue:        queueName,
						Created:      time,
						ClusterId:    reason.PodUnschedulable.ObjectMeta.ExecutorId,
						KubernetesId: reason.PodUnschedulable.ObjectMeta.KubernetesId,
						PodName:      reason.PodUnschedulable.ObjectMeta.Name,
						PodNamespace: reason.PodUnschedulable.ObjectMeta.Namespace,
						Reason:       reason.PodUnschedulable.Message,
						NodeName:     reason.PodUnschedulable.NodeName,
						PodNumber:    reason.PodUnschedulable.PodNumber,
					},
				},
			}
			events = append(events, event)
		case *armadaevents.Error_PodError:
			event := &api.EventMessage{
				Events: &api.EventMessage_Terminated{
					Terminated: &api.JobTerminatedEvent{
						JobId:        jobId,
						JobSetId:     jobSetName,
						Queue:        queueName,
						Created:      time,
						ClusterId:    reason.PodError.ObjectMeta.ExecutorId,
						KubernetesId: reason.PodError.ObjectMeta.KubernetesId,
						PodName:      reason.PodError.ObjectMeta.Name,
						PodNamespace: reason.PodError.ObjectMeta.Namespace,
						Reason:       reason.PodError.Message,
						PodNumber:    reason.PodError.GetPodNumber(),
					},
				},
			}
			events = append(events, event)
		}
	}
	return events, nil
}

func FromJobRunRunning(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunRunning) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromJobRunAssigned(queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunAssigned) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
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

func FromResourceUtilisation(queueName string, jobSetName string, time time.Time, e *armadaevents.ResourceUtilisation) ([]*api.EventMessage, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	apiEvent :=
		&api.JobUtilisationEvent{
			JobId:                 jobId,
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
