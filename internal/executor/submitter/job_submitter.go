package submitter

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/context"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
)

const PodNamePrefix string = "batch-"

type JobSubmitter struct {
	ClusterContext context.ClusterContext
}

func (submitter JobSubmitter) SubmitJob(job *api.Job) (*v1.Pod, error) {
	pod := createPod(job)
	return submitter.ClusterContext.SubmitPod(pod)
}

func createPod(job *api.Job) *v1.Pod {
	labels := createLabels(job)
	setRestartPolicyNever(job.PodSpec)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   PodNamePrefix + job.Id,
			Labels: labels,
		},
		Spec: *job.PodSpec,
	}

	return &pod
}

func setRestartPolicyNever(podSpec *v1.PodSpec) {
	podSpec.RestartPolicy = v1.RestartPolicyNever
}

func createLabels(job *api.Job) map[string]string {
	labels := make(map[string]string)

	labels[domain.JobId] = job.Id
	labels[domain.JobSetId] = job.JobSetId
	labels[domain.Queue] = job.Queue

	return labels
}
