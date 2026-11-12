package job

import (
	"fmt"

	"github.com/armadaproject/armada/internal/lookout/gen/models"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/httpapi"
)

// ListArgs is a request for Lookout's job listing. A job must satisfy every filter. The filters and the order are
// the types of Lookout's own API, so they have the shape and the validation it has.
type ListArgs struct {
	Filters []*models.Filter
	Order   models.Order
	// ActiveJobSets restricts the jobs to those in job sets that are still active.
	ActiveJobSets bool
	// Skip jobs before returning up to Take of them.
	Skip int
	Take int
}

// ListAPI returns the jobs that match the args, in order.
type ListAPI func(args ListArgs) ([]*models.Job, error)

func List(getConnectionDetails client.ConnectionDetails) ListAPI {
	return func(args ListArgs) ([]*models.Job, error) {
		if err := args.Validate(); err != nil {
			return nil, err
		}
		connectionDetails, err := getConnectionDetails()
		if err != nil {
			return nil, fmt.Errorf("failed to obtain api connection details: %s", err)
		}
		api, err := newClient(connectionDetails)
		if err != nil {
			return nil, err
		}

		jobs, err := getJobs(api, args)
		if err != nil {
			return nil, fmt.Errorf("list jobs request failed: %s", err)
		}
		return jobs, nil
	}
}

// getJobs returns the jobs that match the args. The result is never nil.
func getJobs(api *httpapi.Client, args ListArgs) ([]*models.Job, error) {
	request := map[string]any{
		"filters":       requestFilters(args.Filters),
		"order":         args.Order,
		"skip":          args.Skip,
		"take":          args.Take,
		"activeJobSets": args.ActiveJobSets,
	}
	var response struct {
		Jobs []*models.Job `json:"jobs"`
	}
	if err := api.Post("/api/v1/jobs", request, &response); err != nil {
		return nil, err
	}
	if response.Jobs == nil {
		// Lookout sends null when nothing matches.
		response.Jobs = []*models.Job{}
	}
	return response.Jobs, nil
}
