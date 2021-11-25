package armadactl

import (
	"fmt"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// Reprioritize sets the priority of the job identified by (jobId, queueName, jobSet) to priorityFactor
// TODO We should have separate methods to operate on individual jobs and job sets
func (a *App) Reprioritize(jobId string, queueName string, jobSet string, priorityFactor float64) (err error) {
	client.WithConnection(a.Params.ApiConnectionDetails, func(conn *grpc.ClientConn) {
		client := api.NewSubmitClient(conn)

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
		result, err := client.ReprioritizeJobs(ctx, &req)
		if err != nil {
			err = fmt.Errorf("[armadactl.Reprioritize] error submitting reprioritizing request %#v: %s", req, err)
			return
		}

		err = a.writeResults(result.ReprioritizationResults)
		if err != nil {
			err = fmt.Errorf("[armadactl.Reprioritize] error writing reprioritizing results for request %#v: %s", req, err)
			return
		}
	})
	return
}

func (a *App) writeResults(results map[string]string) error {
	if len(results) == 0 {
		return fmt.Errorf("[armadactl.writeResults] no jobs were reprioritized")
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

	// TODO Should we really return an error here? Since we notify the user of errors on a per-job basis.
	if len(erroredIds) > 0 {
		return fmt.Errorf("[armadactl.writeResults] error reprioritizing some jobs")
	}
	return nil
}
