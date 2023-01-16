package armadactl

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// Reprioritize sets the priority of the job identified by (jobId, queueName, jobSet) to priorityFactor
// TODO We should have separate methods to operate on individual jobs and job sets
func (a *App) Reprioritize(jobId string, queueName string, jobSet string, priorityFactor float64) error {
	return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
		var jobIds []string
		if jobId != "" {
			jobIds = append(jobIds, jobId)
		}

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		req := api.JobReprioritizeRequest{
			JobIds:      jobIds,
			JobSetId:    jobSet,
			Queue:       queueName,
			NewPriority: priorityFactor,
		}
		result, err := c.ReprioritizeJobs(ctx, &req)
		if err != nil {
			return errors.WithMessagef(err, "error reprioritising jobs matching queue: %s, job set: %s, and job ID: %s\n", queueName, jobSet, jobId)
		}

		err = a.writeResults(result.ReprioritizationResults)
		if err != nil {
			return err
		}

		return nil
	})
}

func (a *App) writeResults(results map[string]string) error {
	if len(results) == 0 {
		return errors.Errorf("no jobs were reprioritized")
	}

	var reprioritizedIds []string
	erroredIds := make(map[string]string)
	for jobId, errorString := range results {
		if errorString != "" {
			erroredIds[jobId] = errorString
		} else {
			reprioritizedIds = append(reprioritizedIds, jobId)
		}
	}

	if len(reprioritizedIds) > 0 {
		fmt.Fprintf(a.Out, "Reprioritized jobs with ID:\n")
		for _, jobId := range reprioritizedIds {
			fmt.Fprintf(a.Out, "%s\n", jobId)
		}
	}

	if len(erroredIds) > 0 {
		fmt.Fprintf(a.Out, "\n")
		fmt.Fprintf(a.Out, "Failed to reprioritize:\n")
		for jobId, errorString := range erroredIds {
			fmt.Fprintf(a.Out, "%s failed with error %s", jobId, errorString)
		}
	}

	if len(erroredIds) > 0 {
		return errors.Errorf("error reprioritizing some jobs")
	}
	return nil
}
