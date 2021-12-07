package armadactl

import (
	"fmt"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
	"github.com/G-Research/armada/pkg/client/util"
	"github.com/G-Research/armada/pkg/client/validation"
)

// Submit a job, represented by a file, to the Armada server.
// If dry-run is true, the job file is validated but not submitted.
func (a *App) Submit(path string, dryRun bool) (outerErr error) {

	ok, err := validation.ValidateSubmitFile(path)
	if !ok {
		fmt.Fprintf(a.Out, "Invalid jobfile: %s/n", err)
		return nil
	}

	submitFile := &domain.JobSubmitFile{}

	err = util.BindJsonOrYaml(path, submitFile)
	if err != nil {
		return fmt.Errorf("[armadactl.Submit] error parsing job file: %s", err)
	}

	if dryRun {
		return
	}

	requests := client.CreateChunkedSubmitRequests(submitFile.Queue, submitFile.JobSetId, submitFile.Jobs)
	client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) {
		submissionClient := api.NewSubmitClient(conn)
		for _, request := range requests {
			response, err := client.SubmitJobs(submissionClient, request)
			if err != nil {
				outerErr = fmt.Errorf("[armadactl.Submit] error submitting job with request %#v: %s", request, err)
				return
			}

			for _, jobResponseItem := range response.JobResponseItems {
				if jobResponseItem.Error != "" {
					fmt.Fprintf(a.Out, "Error submitting job: %s\n", jobResponseItem.Error)
				} else {
					fmt.Fprintf(a.Out, "Submitted job with ID %s to job set with ID %s\n", jobResponseItem.JobId, request.JobSetId)
				}
			}

		}
	})
	return
}
