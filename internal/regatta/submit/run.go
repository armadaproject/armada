package submit

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// pollInterval controls how often Run checks the last submitted job's terminal status.
const pollInterval = 5 * time.Second

// queueVisibilityRetries/-Delay work around a known Armada race: a freshly created queue isn't
// always immediately visible to the very next submit call on the same connection.
const (
	queueVisibilityRetries = 5
	queueVisibilityDelay   = 1 * time.Second
)

var terminalStates = map[api.JobState]bool{
	api.JobState_SUCCEEDED: true,
	api.JobState_FAILED:    true,
	api.JobState_CANCELLED: true,
	api.JobState_REJECTED:  true,
}

// Run submits spec.Count copies of spec.Spec into spec.Queue/spec.JobSetId, then blocks until
// the last submitted job reaches a terminal state (or ctx is cancelled), as a proxy for the
// whole batch being done.
func Run(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	jobIds, err := submit(apiConnectionDetails, spec)
	if err != nil {
		return fmt.Errorf("submitting jobs: %w", err)
	}
	log.Infof("submitted %d jobs to queue %s", len(jobIds), spec.Queue)
	if len(jobIds) == 0 {
		return nil
	}

	return waitForTerminal(ctx, apiConnectionDetails, jobIds[len(jobIds)-1])
}

func submit(apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) ([]string, error) {
	namespace := spec.Namespace
	if namespace == "" {
		namespace = "default"
	}

	items := make([]*api.JobSubmitRequestItem, spec.Count)
	for i := range items {
		items[i] = &api.JobSubmitRequestItem{
			Namespace: namespace,
			PodSpec:   spec.Spec,
		}
	}

	var jobIds []string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		if err := client.CreateQueue(submitClient, &api.Queue{Name: spec.Queue, PriorityFactor: 1}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("creating queue %s: %w", spec.Queue, err)
		}

		requests := client.CreateChunkedSubmitRequests(spec.Queue, spec.JobSetId, items)
		for _, request := range requests {
			var response *api.JobSubmitResponse
			var err error
			for i := 0; i < queueVisibilityRetries; i++ {
				response, err = client.SubmitJobs(submitClient, request)
				if err == nil || status.Code(err) != codes.PermissionDenied {
					break
				}
				time.Sleep(queueVisibilityDelay)
			}
			if err != nil {
				return fmt.Errorf("submitting jobs: %w", err)
			}
			for _, item := range response.JobResponseItems {
				if item.Error != "" {
					return fmt.Errorf("job rejected: %s", item.Error)
				}
				jobIds = append(jobIds, item.JobId)
			}
		}
		return nil
	})
	return jobIds, err
}

func waitForTerminal(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, jobId string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}

		state, err := getJobState(apiConnectionDetails, jobId)
		if err != nil {
			return fmt.Errorf("polling job status: %w", err)
		}
		if terminalStates[state] {
			return nil
		}
		log.Infof("last submitted job %s still running", jobId)
	}
}

func getJobState(apiConnectionDetails *client.ApiConnectionDetails, jobId string) (api.JobState, error) {
	var state api.JobState
	err := client.WithJobsClient(apiConnectionDetails, func(jobsClient api.JobsClient) error {
		response, err := jobsClient.GetJobStatus(context.Background(), &api.JobStatusRequest{JobIds: []string{jobId}})
		if err != nil {
			return err
		}
		state = response.JobStates[jobId]
		return nil
	})
	return state, err
}
