package submitter

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/model"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strconv"
)

const PodNamePrefix string = "batch_"

type JobSubmitter struct {
	KubernetesClient kubernetes.Interface
}

func (submitter JobSubmitter) SubmitJob(job *model.Job, namespace string) (*v1.Pod, error) {
	pod := createPod(job)

	return submitter.KubernetesClient.CoreV1().Pods(namespace).Create(pod)
}

func createPod(job *model.Job) *v1.Pod {
	labels := createLabels(job)

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:   PodNamePrefix + job.Id,
			Labels: labels,
		},
		Spec: *job.PodSpec,
	}

	return &pod
}

func createLabels(job *model.Job) map[string]string {
	labels := make(map[string]string)

	labels[domain.JobId] = job.Id
	labels[domain.JobSetId] = job.JobSetId
	labels[domain.Queue] = job.Queue
	labels[domain.ReadyForCleanup] = strconv.FormatBool(false)

	return labels
}
