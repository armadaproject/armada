package armadactl

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"google.golang.org/grpc"
)

// Resources prints the resources used by the jobs in job set with ID jobSetId in the given queue.
func (a *App) Resources(queueName string, jobSetId string) error {
	client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) {
		eventsClient := api.NewEventClient(conn)
		state := client.GetJobSetState(eventsClient, queueName, jobSetId, context.Background())

		for _, job := range state.GetCurrentState() {
			fmt.Fprintf(a.Out, "Job ID: %v, maximum used resources: %v\n", job.Job.Id, job.MaxUsedResources)
		}
	})
	return nil
}
