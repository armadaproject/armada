package validation

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/pkg/api"
)

func ValidateJobSetFilter(filter *api.JobSetFilter) error {
	if filter == nil {
		return nil
	}
	providedStatesSet := map[string]bool{}
	for _, state := range filter.States {
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

// ValidateStorableText rejects strings that Postgres text columns cannot store (NUL, invalid
// UTF-8). Such a string would be accepted here, published, and then fail every write of the
// scheduler ingester's batch.
func ValidateStorableText(name, value string) error {
	if !utf8.ValidString(value) || strings.ContainsRune(value, 0) {
		return &armadaerrors.ErrInvalidArgument{Name: name, Value: strconv.Quote(value), Message: "must be valid UTF-8 without NUL characters"}
	}
	return nil
}

// ValidateReason rejects reasons longer than MaxReasonBytes. Length is measured in
// bytes, consistent with util.Truncate which slices reasons by byte.
func ValidateReason(req ReasonRequest) error {
	if err := ValidateStorableText("Reason", req.GetReason()); err != nil {
		return err
	}
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
	if err := ValidateStorableText("JobSetId", req.GetJobSetId()); err != nil {
		return err
	}
	return nil
}
