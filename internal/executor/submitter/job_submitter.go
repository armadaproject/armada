package submitter

import (
	"github.com/G-Research/k8s-batch/internal/model"
	"k8s.io/client-go/kubernetes"
)

type JobSubmitter struct {
	KubernetesClient kubernetes.Interface
}

func (submitter JobSubmitter) SubmitJob(job *model.Job, namespace string) {
}
