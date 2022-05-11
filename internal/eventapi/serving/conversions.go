package serving

import (
	"time"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/pkg/errors"
)

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
