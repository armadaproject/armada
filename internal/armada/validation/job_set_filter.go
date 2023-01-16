package validation

import (
	"fmt"

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
