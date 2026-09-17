package submit

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/regatta/config"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// pollInterval controls how often Run checks submitted jobs' terminal status.
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

// Run submits spec's jobs into spec.Queue/spec.JobSetId and blocks until they're done, either
// all at once (Mode "" / "one-shot") or spread out over time (Mode "ramp-up").
func Run(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	if spec.Mode == config.LoadModeRampUp {
		return runRampUp(ctx, apiConnectionDetails, spec)
	}
	return runOneShot(ctx, apiConnectionDetails, spec)
}

// runOneShot submits every job in one shot, then blocks until the last submitted job reaches a
// terminal state (or ctx is cancelled), as a proxy for the whole batch being done.
func runOneShot(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	jobIds, err := submitItems(apiConnectionDetails, spec.Queue, spec.JobSetId, spec.Namespace, spec.Jobs)
	if err != nil {
		return fmt.Errorf("submitting jobs: %w", err)
	}
	log.Infof("submitted %d jobs to queue %s", len(jobIds), spec.Queue)
	if len(jobIds) == 0 {
		return nil
	}

	return waitForTerminal(ctx, apiConnectionDetails, []string{jobIds[len(jobIds)-1]})
}

// submitItems submits count copies of each JobItem's PodSpec, in list order, returning every
// submitted job ID.
func submitItems(apiConnectionDetails *client.ApiConnectionDetails, queue, jobSetId, namespace string, jobs []JobItem) ([]string, error) {
	if namespace == "" {
		namespace = "default"
	}

	var items []*api.JobSubmitRequestItem
	for _, job := range jobs {
		for i := 0; i < job.Count; i++ {
			items = append(items, &api.JobSubmitRequestItem{
				Namespace: namespace,
				PodSpec:   job.Spec,
			})
		}
	}

	var jobIds []string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		if err := client.CreateQueue(submitClient, &api.Queue{Name: queue, PriorityFactor: 1}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("creating queue %s: %w", queue, err)
		}

		requests := client.CreateChunkedSubmitRequests(queue, jobSetId, items)
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

// waitForTerminal blocks until every job in jobIds has reached a terminal state, or ctx is
// cancelled.
func waitForTerminal(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, jobIds []string) error {
	pending := map[string]bool{}
	for _, id := range jobIds {
		pending[id] = true
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}

		states, err := getJobStates(apiConnectionDetails, jobIds)
		if err != nil {
			return fmt.Errorf("polling job status: %w", err)
		}
		for id := range pending {
			if terminalStates[states[id]] {
				delete(pending, id)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		log.Infof("%d/%d submitted jobs still running", len(pending), len(jobIds))
	}
}

func getJobStates(apiConnectionDetails *client.ApiConnectionDetails, jobIds []string) (map[string]api.JobState, error) {
	var states map[string]api.JobState
	err := client.WithJobsClient(apiConnectionDetails, func(jobsClient api.JobsClient) error {
		response, err := jobsClient.GetJobStatus(context.Background(), &api.JobStatusRequest{JobIds: jobIds})
		if err != nil {
			return err
		}
		states = response.JobStates
		return nil
	})
	return states, err
}
