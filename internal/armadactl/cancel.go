package armadactl

import (
	"fmt"
	"strings"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// Cancel cancels a job.
// TODO this method does too much; there should be separate methods to cancel individual jobs and all jobs in a job set
func (a *App) Cancel(queue string, jobSetId string, jobId string) (outerErr error) {
	apiConnectionDetails := a.Params.ApiConnectionDetails

	fmt.Fprintf(a.Out, "Requesting cancellation of jobs matching queue: %s, job set: %s, and job ID: %s\n", queue, jobSetId, jobId)

	if a.Params.DryRun {
		return
	}

	client.WithConnection(apiConnectionDetails, func(conn *grpc.ClientConn) {
		client := api.NewSubmitClient(conn)
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		result, err := client.CancelJobs(ctx, &api.JobCancelRequest{
			JobId:    jobId,
			JobSetId: jobSetId,
			Queue:    queue,
		})

		if err != nil {
			outerErr = fmt.Errorf("[armadactl.Cencel] error cancelling job: %s", err)
			return
		}
		fmt.Fprintf(a.Out, "Requested cancellation for jobs %s\n", strings.Join(result.CancelledIds, ", "))
	})
	return
}
