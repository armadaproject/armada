package armadactl

import (
	"fmt"

	"github.com/armadaproject/armada/internal/common/context"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Resources prints the resources used by the jobs in job set with ID jobSetId in the given queue.
func (a *App) Resources(queueName string, jobSetId string) error {
	return client.WithEventClient(a.Params.ApiConnectionDetails, func(c api.EventClient) error {
		state := client.GetJobSetState(c, queueName, jobSetId, context.Background(), true, false, false)

		for _, job := range state.GetCurrentState() {
			fmt.Fprintf(a.Out, "Job ID: %v, maximum used resources: %v\n", job.Job.Id, job.MaxUsedResources)
		}
		return nil
	})
}
