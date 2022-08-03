package validation

import (
	"fmt"

	"github.com/G-Research/armada/internal/armada/domain"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/pkg/api"
)

func ValidateJobSetFilter(filter *api.JobSetFilter) error {
	if filter == nil {
		return nil
	}
	providedStatesSet := util.StringListToSet(filter.State)
	for _, state := range filter.State {
		if state != domain.QueuedPhase && state != domain.PendingPhase && state != domain.RunningPhase {
			return fmt.Errorf("invalid state provided - state %s unrecognised", state)
		}

		if state == domain.PendingPhase {
			if _, present := providedStatesSet[domain.RunningPhase]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together", domain.PendingPhase, domain.RunningPhase)
			}
		}

		if state == domain.RunningPhase {
			if _, present := providedStatesSet[domain.PendingPhase]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together", domain.PendingPhase, domain.RunningPhase)
			}
		}
	}

	return nil
}
