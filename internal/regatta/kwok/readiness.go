package kwok

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

const (
	probeQueue    = "regatta"
	probeJobSetId = "regatta-queue-readiness"
	jobIdPodLabel = "armada_job_id"
)

// ProbeConfig controls WaitUntilSchedulable's retry behaviour.
type ProbeConfig struct {
	Retries      int           // number of canary-job attempts before giving up
	InitialDelay time.Duration // delay before the first poll of an attempt; doubles each retry
}

// WaitUntilSchedulable submits a throwaway canary job that only tolerates the KWOK fake-node
// taint, and blocks until Armada actually schedules it onto a fake node (i.e. until the
// executor has reported the node's capacity to the scheduler - K8s "Ready" alone doesn't mean
// the scheduler knows about it yet, since the executor only reports on its own poll interval).
// Retries cfg.Retries times, doubling the poll delay each attempt, since there's no direct way
// to ask the scheduler "do you know about this node yet". A submit-time rejection (e.g. "no
// node matches this pod's selector yet") is expected on early attempts, before the executor's
// next report cycle - treated the same as "didn't land on a fake node in time", not fatal.
func WaitUntilSchedulable(ctx context.Context, kubeClient kubernetes.Interface, apiConnectionDetails *client.ApiConnectionDetails, cfg ProbeConfig, targetName string) error {
	delay := cfg.InitialDelay
	var lastErr error
	for attempt := 1; attempt <= cfg.Retries; attempt++ {
		jobId, err := submitCanaryJob(apiConnectionDetails, targetName)
		if err != nil {
			lastErr = err
		} else {
			select {
			case <-ctx.Done():
				cancelCanaryJob(apiConnectionDetails, jobId)
				return ctx.Err()
			case <-time.After(delay):
			}
			var scheduled bool
			scheduled, err = canaryRunningOnFakeNode(ctx, kubeClient, jobId)
			cancelCanaryJob(apiConnectionDetails, jobId)
			if err != nil {
				lastErr = err
			} else if scheduled {
				return nil
			} else {
				lastErr = fmt.Errorf("canary job %s did not land on a fake node within %s (attempt %d/%d)", jobId, delay, attempt, cfg.Retries)
			}
		}

		delay *= 2
	}
	return fmt.Errorf("fake nodes never became schedulable after %d attempts: %w", cfg.Retries, lastErr)
}

// queueVisibilityRetries/-Delay work around a known Armada race: a freshly created queue isn't
// always immediately visible to the very next submit call on the same connection.
const (
	queueVisibilityRetries = 6
	queueVisibilityDelay   = 1 * time.Second
)

func submitCanaryJob(apiConnectionDetails *client.ApiConnectionDetails, targetName string) (string, error) {
	var jobId string
	err := client.WithSubmitClient(apiConnectionDetails, func(submitClient api.SubmitClient) error {
		if err := client.CreateQueue(submitClient, &api.Queue{Name: probeQueue, PriorityFactor: 1}); err != nil && status.Code(err) != codes.AlreadyExists {
			return fmt.Errorf("creating probe queue: %w", err)
		}

		requests := client.CreateChunkedSubmitRequests(probeQueue, probeJobSetId, []*api.JobSubmitRequestItem{canaryJobSpec(targetName)})
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

func canaryJobSpec(targetName string) *api.JobSubmitRequestItem {
	cpu := resource.MustParse("10m")
	memory := resource.MustParse("8Mi")
	return &api.JobSubmitRequestItem{
		Namespace: "default",
		PodSpec: &v1.PodSpec{
			TerminationGracePeriodSeconds: pointerTo(int64(0)),
			RestartPolicy:                 v1.RestartPolicyNever,
			NodeSelector: map[string]string{
				NodeAnnotation: NodeAnnotationOK,
				TargetLabel:    targetName,
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

func canaryRunningOnFakeNode(ctx context.Context, kubeClient kubernetes.Interface, jobId string) (bool, error) {
	pods, err := kubeClient.CoreV1().Pods("default").List(ctx, metav1.ListOptions{
		LabelSelector: jobIdPodLabel + "=" + jobId,
	})
	if err != nil {
		return false, fmt.Errorf("listing canary pods: %w", err)
	}
	for _, pod := range pods.Items {
		if pod.Spec.NodeName == "" {
			continue
		}
		node, err := kubeClient.CoreV1().Nodes().Get(ctx, pod.Spec.NodeName, metav1.GetOptions{})
		if err != nil {
			continue
		}
		if node.Labels[NodeAnnotation] == NodeAnnotationOK && (pod.Status.Phase == v1.PodRunning || pod.Status.Phase == v1.PodSucceeded) {
			return true, nil
		}
	}
	return false, nil
}

func pointerTo[T any](v T) *T {
	return &v
}
