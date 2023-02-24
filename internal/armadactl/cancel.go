package armadactl

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Cancel cancels a job.
// TODO this method does too much; there should be separate methods to cancel individual jobs and all jobs in a job set
func (a *App) Cancel(queue string, jobSetId string, jobId string) (outerErr error) {
	apiConnectionDetails := a.Params.ApiConnectionDetails

	fmt.Fprintf(a.Out, "Requesting cancellation of jobs matching queue: %s, job set: %s, and job ID: %s\n", queue, jobSetId, jobId)
	return client.WithSubmitClient(apiConnectionDetails, func(c api.SubmitClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		result, err := c.CancelJobs(ctx, &api.JobCancelRequest{
			JobId:    jobId,
			JobSetId: jobSetId,
			Queue:    queue,
		})
		if err != nil {
			return errors.Wrapf(err, "error cancelling jobs matching queue: %s, job set: %s, and job id: %s", queue, jobSetId, jobId)
		}

		fmt.Fprintf(a.Out, "Requested cancellation for jobs %s\n", strings.Join(result.CancelledIds, ", "))
		return nil
	})
}
