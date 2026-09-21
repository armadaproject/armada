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

// queueVisibilityRetries/-Delay work around a known Armada race: a freshly created queue isn't
// always immediately visible to the very next submit call on the same connection.
const (
	queueVisibilityRetries = 5
	queueVisibilityDelay   = 1 * time.Second
)

// Run submits spec's jobs into spec.Queue/spec.JobSetId, either all at once (Mode "" /
// "one-shot") or spread out over time (Mode "ramp-up"), and returns once submission is done.
// Regatta is a load generator, not a job-completion tracker: it never polls job state, so it
// scales to submission batches far larger than would be practical to poll individually. Fake
// nodes/executors started for the run are left running - tear them down explicitly with
// `regatta teardown` once you're done collecting metrics.
func Run(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	if spec.Mode == config.LoadModeRampUp {
		return runRampUp(ctx, apiConnectionDetails, spec)
	}
	return runOneShot(apiConnectionDetails, spec)
}

// runOneShot submits every job in one shot and returns.
func runOneShot(apiConnectionDetails *client.ApiConnectionDetails, spec *Spec) error {
	jobIds, err := submitItems(apiConnectionDetails, spec.Queue, spec.JobSetId, spec.Namespace, spec.Jobs)
	if err != nil {
		return fmt.Errorf("submitting jobs: %w", err)
	}
	log.Infof("submitted %d jobs to queue %s", len(jobIds), spec.Queue)
	return nil
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
