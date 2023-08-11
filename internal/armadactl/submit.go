package armadactl

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
	"github.com/armadaproject/armada/pkg/client/util"
	"github.com/armadaproject/armada/pkg/client/validation"
)

// Submit a job, represented by a file, to the Armada server.
// If dry-run is true, the job file is validated but not submitted.
func (a *App) Submit(path string, dryRun bool) error {
	ok, err := validation.ValidateSubmitFile(path)
	if !ok {
		return err
	}

	submitFile := &domain.JobSubmitFile{}
	err = util.BindJsonOrYaml(path, submitFile)
	if err != nil {
		return err
	}

	if dryRun {
		return nil
	}

	requests := client.CreateChunkedSubmitRequests(submitFile.Queue, submitFile.JobSetId, submitFile.Jobs)
	return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(originalClient api.SubmitClient) error {
		c := api.CustomSubmitClient{Inner: originalClient}

		for _, request := range requests {
			response, err := client.CustomClientSubmitJobs(c, request)
			if err != nil {
				if response != nil {
					fmt.Fprintln(a.Out, "[JobSubmitResponse]")
					for _, jobResponseItem := range response.JobResponseItems {
						fmt.Fprintf(a.Out, "Error submitting job with id %s, details: %s\n", jobResponseItem.JobId, jobResponseItem.Error)
					}
				}
				fmt.Fprintln(a.Out, "[Error]")
				return errors.WithMessagef(err, "error submitting request %#v", request)
			}

			for _, jobResponseItem := range response.JobResponseItems {
				if jobResponseItem.Error != "" {
					fmt.Fprintf(a.Out, "Error submitting job: %s\n", jobResponseItem.Error)
				} else {
					fmt.Fprintf(a.Out, "Submitted job with id %s to job set %s\n", jobResponseItem.JobId, request.JobSetId)
				}
			}
		}
		return nil
	})
}
