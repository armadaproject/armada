package armadaevents

import (
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func JobIdFromEvent(event *EventSequence_Event) (*Uuid, error) {
	switch e := event.Event.(type) {
	case *EventSequence_Event_SubmitJob:
		return e.SubmitJob.JobId, nil
	case *EventSequence_Event_ReprioritiseJob:
		return e.ReprioritiseJob.JobId, nil
	case *EventSequence_Event_ReprioritisedJob:
		return e.ReprioritisedJob.JobId, nil
	case *EventSequence_Event_CancelJob:
		return e.CancelJob.JobId, nil
	case *EventSequence_Event_CancelledJob:
		return e.CancelledJob.JobId, nil
	case *EventSequence_Event_JobSucceeded:
		return e.JobSucceeded.JobId, nil
	case *EventSequence_Event_JobRunSucceeded:
		return e.JobRunSucceeded.JobId, nil
	case *EventSequence_Event_JobRunLeased:
		return e.JobRunLeased.JobId, nil
	case *EventSequence_Event_JobRunAssigned:
		return e.JobRunAssigned.JobId, nil
	case *EventSequence_Event_JobRunRunning:
		return e.JobRunRunning.JobId, nil
	case *EventSequence_Event_JobErrors:
		return e.JobErrors.JobId, nil
	case *EventSequence_Event_JobRunErrors:
		return e.JobRunErrors.JobId, nil
	case *EventSequence_Event_JobDuplicateDetected:
		return e.JobDuplicateDetected.NewJobId, nil
	case *EventSequence_Event_StandaloneIngressInfo:
		return e.StandaloneIngressInfo.JobId, nil
	default:
		err := errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "event.Event",
			Value:   e,
			Message: "event doesn't contain a jobId",
		})
		return &Uuid{
			High64: 0,
			Low64:  0,
		}, err
	}
}
