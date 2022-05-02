package eventMessages

import (
	"time"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/pkg/errors"
)

func FromLogSubmit(queueName string, jobSetName string, time time.Time, e *armadaevents.SubmitJob) ([]api.Event, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	return []api.Event{&api.JobQueuedEvent{
		JobId:    jobId,
		JobSetId: jobSetName,
		Queue:    queueName,
		Created:  time,
	}}, nil
}

func FromLogCancelled(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.CancelledJob) (*api.JobCancelledEvent, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	return &api.JobCancelledEvent{
		JobId:     jobId,
		JobSetId:  jobSetName,
		Queue:     queueName,
		Created:   time,
		Requestor: userId,
	}, nil
}

func FromLogReprioritised(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.ReprioritisedJob) (*api.JobReprioritizedEvent, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	return &api.JobReprioritizedEvent{
		JobId:       jobId,
		JobSetId:    jobSetName,
		Queue:       queueName,
		Created:     time,
		NewPriority: float64(e.Priority),
		Requestor:   userId,
	}, nil
}

func FromLogDuplicateDetected(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobDuplicateDetected) (*api.JobDuplicateFoundEvent, error) {
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
	return &api.JobDuplicateFoundEvent{
		JobId:         jobId,
		JobSetId:      jobSetName,
		Queue:         queueName,
		Created:       time,
		OriginalJobId: originalJobId,
	}, nil
}

func FromLogJobRunLeased(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunLeased) (*api.JobLeasedEvent, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}
	return &api.JobLeasedEvent{
		JobId:     jobId,
		JobSetId:  jobSetName,
		Queue:     queueName,
		Created:   time,
		ClusterId: e.ExecutorId,
	}, nil
}

func FromJobRunErrors(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunErrors) ([]api.Event, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	events := make([]api.Event, 0)
	for _, msgErr := range e.GetErrors() {
		if msgErr.Terminal {
			switch reason := msgErr.Reason.(type) {
			case *armadaevents.Error_LeaseExpired:
				event := &api.JobLeaseExpiredEvent{
					JobId:    jobId,
					JobSetId: jobSetName,
					Queue:    queueName,
					Created:  time,
				}
				events = append(events, event)
			case *armadaevents.Error_PodError:
				event := makeJobFailed(jobId, queueName, jobSetName, time, reason)
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

func FromJobErrors(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobErrors) ([]api.Event, error) {
	jobId, err := armadaevents.UlidStringFromProtoUuid(e.JobId)
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	events := make([]api.Event, 0)
	for _, msgErr := range e.GetErrors() {
		switch reason := msgErr.Reason.(type) {
		case *armadaevents.Error_PodUnschedulable:
			event := api.JobUnableToScheduleEvent{
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
			}
			events = append(events, &event)
		case *armadaevents.Error_PodError:
			event := api.JobTerminatedEvent{
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
			}
			events = append(events, &event)
		}

	}
	return events, nil
}

func FromJobRunRunning(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunRunning) (*api.JobRunningEvent, error) {
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

	return apiEvent, nil
}

func FromJobRunAssigned(userId string, queueName string, jobSetName string, time time.Time, e *armadaevents.JobRunAssigned) (*api.JobPendingEvent, error) {
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
	return apiEvent, nil
}
