package armadaevents

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func JobIdFromEvent(event *EventSequence_Event) (string, error) {
	switch e := event.Event.(type) {
	case *EventSequence_Event_SubmitJob:
		return e.SubmitJob.JobIdStr, nil
	case *EventSequence_Event_ReprioritiseJob:
		return e.ReprioritiseJob.JobIdStr, nil
	case *EventSequence_Event_ReprioritisedJob:
		return e.ReprioritisedJob.JobIdStr, nil
	case *EventSequence_Event_CancelJob:
		return e.CancelJob.JobIdStr, nil
	case *EventSequence_Event_CancelledJob:
		return e.CancelledJob.JobIdStr, nil
	case *EventSequence_Event_JobSucceeded:
		return e.JobSucceeded.JobIdStr, nil
	case *EventSequence_Event_JobRunSucceeded:
		return e.JobRunSucceeded.JobIdStr, nil
	case *EventSequence_Event_JobRunLeased:
		return e.JobRunLeased.JobIdStr, nil
	case *EventSequence_Event_JobRunAssigned:
		return e.JobRunAssigned.JobIdStr, nil
	case *EventSequence_Event_JobRunRunning:
		return e.JobRunRunning.JobIdStr, nil
	case *EventSequence_Event_JobErrors:
		return e.JobErrors.JobIdStr, nil
	case *EventSequence_Event_JobRunErrors:
		return e.JobRunErrors.JobIdStr, nil
	case *EventSequence_Event_StandaloneIngressInfo:
		return e.StandaloneIngressInfo.JobIdStr, nil
	case *EventSequence_Event_JobRunPreempted:
		return e.JobRunPreempted.PreemptedJobIdStr, nil
	case *EventSequence_Event_JobRunCancelled:
		return e.JobRunCancelled.JobIdStr, nil
	case *EventSequence_Event_JobRequeued:
		return e.JobRequeued.JobIdStr, nil
	case *EventSequence_Event_JobValidated:
		return e.JobValidated.JobIdStr, nil
	default:
		err := errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "event.Event",
			Value:   e,
			Message: "event doesn't contain a jobId",
		})
		return "", err
	}
}
