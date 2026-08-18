package armadactl

import (
	"fmt"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// ReprioritiseJobSet sets the priority of the jobSet identified by (queueName, jobSet) to priorityFactor
func (a *App) ReprioritiseJobSet(queueName string, jobSet string, priorityFactor float64) error {
	return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		req := api.JobReprioritiseRequest{
			JobSetId:    jobSet,
			Queue:       queueName,
			NewPriority: priorityFactor,
		}
		result, err := c.ReprioritiseJobs(ctx, &req)
		if err != nil {
			return errors.WithMessagef(err, "error reprioritising jobs matching queue: %s, job set: %s\n", queueName, jobSet)
		}

		err = a.writeResults(result.ReprioritisationResults)
		if err != nil {
			return err
		}

		return nil
	})
}

// Reprioritise sets the priority of the job identified by (jobId) to priorityFactor
func (a *App) ReprioritiseJob(queue string, jobSet string, jobId string, priorityFactor float64) error {
	return client.WithSubmitClient(a.Params.ApiConnectionDetails, func(c api.SubmitClient) error {
		var jobIds []string
		if jobId != "" {
			jobIds = append(jobIds, jobId)
		}

		ctx, cancel := common.ContextWithDefaultTimeout()
		defer cancel()

		req := api.JobReprioritiseRequest{
			Queue:       queue,
			JobSetId:    jobSet,
			JobIds:      jobIds,
			NewPriority: priorityFactor,
		}
		result, err := c.ReprioritiseJobs(ctx, &req)
		if err != nil {
			return errors.WithMessagef(err, "error reprioritising jobs matching job ID: %s\n", jobId)
		}

		err = a.writeResults(result.ReprioritisationResults)
		if err != nil {
			return err
		}

		return nil
	})
}

func (a *App) writeResults(results map[string]string) error {
	if len(results) == 0 {
		return errors.Errorf("no jobs were reprioritised")
	}

	var reprioritisedIds []string
	erroredIds := make(map[string]string)
	for jobId, errorString := range results {
		if errorString != "" {
			erroredIds[jobId] = errorString
		} else {
			reprioritisedIds = append(reprioritisedIds, jobId)
		}
	}

	if len(reprioritisedIds) > 0 {
		fmt.Fprintf(a.Out, "Reprioritised jobs with ID:\n")
		for _, jobId := range reprioritisedIds {
			fmt.Fprintf(a.Out, "%s\n", jobId)
		}
	}

	if len(erroredIds) > 0 {
		fmt.Fprintf(a.Out, "\n")
		fmt.Fprintf(a.Out, "Failed to reprioritise:\n")
		for jobId, errorString := range erroredIds {
			fmt.Fprintf(a.Out, "%s failed with error %s", jobId, errorString)
		}
	}

	if len(erroredIds) > 0 {
		return errors.Errorf("error reprioritising some jobs")
	}
	return nil
}
