package armadactl

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Preempt a job.
func (a *App) Preempt(queue string, jobSetId string, jobId string, reason string) (outerErr error) {
	apiConnectionDetails := a.Params.ApiConnectionDetails

	fmt.Fprintf(a.Out, "Requesting preemption of job matching queue: %s, job set: %s, and job Id: %s\n", queue, jobSetId, jobId)
	return client.WithSubmitClient(apiConnectionDetails, func(c api.SubmitClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		_, err := c.PreemptJobs(ctx, &api.JobPreemptRequest{
			JobIds:   []string{jobId},
			JobSetId: jobSetId,
			Queue:    queue,
			Reason:   reason,
		})
		if err != nil {
			return errors.Wrapf(err, "error preempting job matching queue: %s, job set: %s, and job id: %s", queue, jobSetId, jobId)
		}

		fmt.Fprintf(a.Out, "Requested preemption for job %s\n", jobId)
		return nil
	})
}
