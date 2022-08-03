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
		if !domain.IsValidFilterState(state) {
			return fmt.Errorf("invalid state provided - state %s unrecognised", state)
		}

		if state == domain.Pending.String() {
			if _, present := providedStatesSet[domain.Running.String()]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together", domain.Pending, domain.Running)
			}
		}

		if state == domain.Running.String() {
			if _, present := providedStatesSet[domain.Pending.String()]; !present {
				return fmt.Errorf("unsupported state combination - state %s and %s must always be used together", domain.Pending, domain.Running)
			}
		}
	}

	return nil
}
