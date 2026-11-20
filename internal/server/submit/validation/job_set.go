package validation

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/pkg/api"
)

func ValidateJobSetFilter(filter *api.JobSetFilter) error {
	if filter == nil {
		return nil
	}
	providedStatesSet := map[string]bool{}
	for _, state := range filter.States {
		// Only these states have an equivalent in armadaevents.JobState. Any other state would be
		// converted to its zero value (QUEUED), and jobs the caller did not select would be cancelled.
		switch state {
		case api.JobState_QUEUED, api.JobState_PENDING, api.JobState_RUNNING:
		default:
			return fmt.Errorf("unsupported state %s - a job set can only be filtered by states %s, %s and %s",
				state, api.JobState_QUEUED, api.JobState_PENDING, api.JobState_RUNNING)
		}
		providedStatesSet[state.String()] = true
	}
	for _, state := range filter.States {
		if state == api.JobState_PENDING {
			if _, present := providedStatesSet[api.JobState_RUNNING.String()]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together",
					api.JobState_PENDING, api.JobState_RUNNING)
			}
		}

		if state == api.JobState_RUNNING {
			if _, present := providedStatesSet[api.JobState_PENDING.String()]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together",
					api.JobState_PENDING, api.JobState_RUNNING)
			}
		}
	}

	return nil
}

const MaxReasonBytes = 50

type ReasonRequest interface {
	GetReason() string
}

// ValidateReason rejects reasons longer than MaxReasonBytes. Length is measured in
// bytes, consistent with util.Truncate which slices reasons by byte.
func ValidateReason(req ReasonRequest) error {
	if len(req.GetReason()) > MaxReasonBytes {
		return &armadaerrors.ErrInvalidArgument{
			Name:    "Reason",
			Value:   req.GetReason()[:MaxReasonBytes] + "...",
			Message: fmt.Sprintf("reason cannot be longer than %d bytes", MaxReasonBytes),
		}
	}
	return nil
}

type JobSetRequest interface {
	GetJobSetId() string
	GetQueue() string
}

func ValidateQueueAndJobSet(req JobSetRequest) error {
	if req.GetQueue() == "" {
		return &armadaerrors.ErrInvalidArgument{
			Name:    "Queue",
			Value:   req.GetQueue(),
			Message: "queue cannot be empty",
		}
	}
	if req.GetJobSetId() == "" {
		return &armadaerrors.ErrInvalidArgument{
			Name:    "JobSetId",
			Value:   req.GetJobSetId(),
			Message: "jobset cannot be empty",
		}
	}
	return nil
}
