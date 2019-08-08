package submitter

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const PodNamePrefix string = "batch-"

type JobSubmitter struct {
	KubernetesClient kubernetes.Interface
}

func (submitter JobSubmitter) SubmitJob(job *api.Job) (*v1.Pod, error) {
	pod := createPod(job)

	//TODO Remove hardcoded namespace once it can be user specified
	return submitter.KubernetesClient.CoreV1().Pods("default").Create(pod)
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

//TODO
// Termination grace period seconds to 0
// Image pull policy IfNotPresent
// RunAsUser
