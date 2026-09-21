package fakeexecutor

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/domain"
)

const (
	probeQueue    = "regatta"
	probeJobSetId = "regatta-fakeexecutor-readiness"
)

// ProbeConfig controls WaitUntilSchedulable's retry behaviour. Mirrors kwok.ProbeConfig.
type ProbeConfig struct {
	Retries      int           // number of canary-job attempts before giving up
	InitialDelay time.Duration // delay before the first poll of an attempt; doubles each retry
}

// WaitUntilSchedulable submits a throwaway canary job that only tolerates the fake-executor's
// own simulated-node taint, and blocks until Armada actually reports it Running - i.e. until
// this armada-fakeexecutor process has registered with the scheduler and reported its simulated
// nodes' capacity, not just that the OS process has started (fakeexecutor.Start returns as soon
// as the process forks, well before gRPC registration completes). Retries cfg.Retries times,
// doubling the poll delay each attempt, mirroring kwok.WaitUntilSchedulable - there is no direct
// way to ask the scheduler "do you know about this fake-executor's capacity yet" either.
func WaitUntilSchedulable(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, cfg ProbeConfig, targetName string) error {
	delay := cfg.InitialDelay
	var lastErr error
	for attempt := 1; attempt <= cfg.Retries; attempt++ {
		jobId, err := submitCanaryJob(apiConnectionDetails)
		if err != nil {
			lastErr = err
		} else {
			running, err := waitForCanary(ctx, apiConnectionDetails, jobId, delay)
			cancelCanaryJob(apiConnectionDetails, jobId)
			if err != nil {
				lastErr = err
			} else if running {
				return nil
			} else {
				lastErr = fmt.Errorf("canary job %s did not start running within %s (attempt %d/%d)", jobId, delay, attempt, cfg.Retries)
			}
		}

		delay *= 2
	}
	return fmt.Errorf("target %q never became schedulable after %d attempts: %w", targetName, cfg.Retries, lastErr)
}

// queueVisibilityRetries/-Delay work around a known Armada race: a freshly created queue isn't
// always immediately visible to the very next submit call on the same connection.
const (
	queueVisibilityRetries = 6
	queueVisibilityDelay   = 1 * time.Second
)

func submitCanaryJob(apiConnectionDetails *client.ApiConnectionDetails) (string, error) {
	var jobId string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		if err := client.CreateQueue(submitClient, &api.Queue{Name: probeQueue, PriorityFactor: 1}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("creating probe queue: %w", err)
		}

		requests := client.CreateChunkedSubmitRequests(probeQueue, probeJobSetId, []*api.JobSubmitRequestItem{canaryJobSpec()})
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
				return fmt.Errorf("submitting canary job: %w", err)
			}
			for _, item := range response.JobResponseItems {
				if item.Error != "" {
					return fmt.Errorf("canary job rejected: %s", item.Error)
				}
				jobId = item.JobId
			}
		}
		return nil
	})
	return jobId, err
}

func cancelCanaryJob(apiConnectionDetails *client.ApiConnectionDetails, jobId string) {
	_ = client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		_, err := submitClient.CancelJobs(context.Background(), &api.JobCancelRequest{
			JobId:    jobId,
			JobSetId: probeJobSetId,
			Queue:    probeQueue,
		})
		return err
	})
}

func canaryJobSpec() *api.JobSubmitRequestItem {
	cpu := resource.MustParse("10m")
	memory := resource.MustParse("8Mi")
	return &api.JobSubmitRequestItem{
		Namespace: "default",
		PodSpec: &v1.PodSpec{
			TerminationGracePeriodSeconds: pointerTo(int64(0)),
			RestartPolicy:                 v1.RestartPolicyNever,
			NodeSelector: map[string]string{
				NodeAnnotation: NodeAnnotationOK,
			},
			Tolerations: []v1.Toleration{
				{
					Key:      NodeAnnotation,
					Operator: v1.TolerationOpEqual,
					Value:    NodeAnnotationOK,
					Effect:   v1.TaintEffectNoSchedule,
				},
			},
			Containers: []v1.Container{
				{
					Name:    "probe",
					Image:   "alpine:3.21.3",
					Command: []string{"sh"},
					Args:    []string{"-c", "sleep 10"},
					Resources: v1.ResourceRequirements{
						Limits:   v1.ResourceList{v1.ResourceCPU: cpu, v1.ResourceMemory: memory},
						Requests: v1.ResourceList{v1.ResourceCPU: cpu, v1.ResourceMemory: memory},
					},
				},
			},
		},
	}
}

// waitForCanary tails the canary job's own event stream (there is no Kubernetes API to poll
// against, unlike kwok's WaitUntilSchedulable - armada-fakeexecutor simulates nodes/pods
// entirely in-process over gRPC) until it reaches Running/Succeeded (schedulable) or a terminal
// failure state, or delay elapses. Uses Watch mode (tail, not snapshot) since the canary's own
// submit event may not be indexed yet at the moment this call starts.
func waitForCanary(ctx context.Context, apiConnectionDetails *client.ApiConnectionDetails, jobId string, delay time.Duration) (bool, error) {
	watchCtx, cancel := context.WithTimeout(ctx, delay)
	defer cancel()

	var running bool
	err := client.WithEventClient(apiConnectionDetails, func(eventClient api.EventClient) error {
		client.WatchJobSetWithJobIdsFilter(eventClient, probeQueue, probeJobSetId, true, false, false, false, []string{jobId}, watchCtx, func(state *domain.WatchContext, _ api.Event) bool {
			info := state.GetJobInfo(jobId)
			if info == nil {
				return false
			}
			switch info.Status {
			case domain.Running, domain.Succeeded:
				running = true
				return true
			case domain.Failed, domain.Cancelled:
				return true
			default:
				return false
			}
		})
		return nil
	})
	if err != nil {
		return false, err
	}
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	return running, nil
}

func pointerTo[T any](v T) *T {
	return &v
}
