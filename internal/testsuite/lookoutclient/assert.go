package lookoutclient

import (
	"context"
	"fmt"
	"strings"

	"github.com/armadaproject/armada/internal/common/util"
)

// AssertCategories fetches each job's last run from Lookout and verifies that
// all expectedCategories are present. Skipped if expectedCategories is empty.
// Returns an error if lookoutURL is empty but expectedCategories is non-empty.
func AssertCategories(ctx context.Context, lookoutURL string, jobIDs []string, expectedCategories []string) error {
	if len(expectedCategories) == 0 {
		return nil
	}
	if lookoutURL == "" {
		return fmt.Errorf("failure.categories is set but lookoutUrl is not configured")
	}

	for _, jobID := range jobIDs {
		actual, err := FetchLastRunCategories(ctx, lookoutURL, jobID)
		if err != nil {
			return fmt.Errorf("job %s: %w", jobID, err)
		}

		missing := util.SubtractStringList(expectedCategories, actual)
		if len(missing) > 0 {
			return fmt.Errorf(
				"job %s: expected categories [%s] not found in actual [%s]",
				jobID,
				strings.Join(missing, ", "),
				strings.Join(actual, ", "),
			)
		}
	}
	return nil
}
