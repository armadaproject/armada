package task

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"strconv"
	"strings"
)

type PodDeletionTask struct {
	KubernetesClient kubernetes.Clientset
}

func (deletionTask PodDeletionTask) Run() {
	deleteOptions := createPodDeletionDeleteOptions()
	listOptions := createPodDeletionListOptions()

	err := deletionTask.KubernetesClient.CoreV1().Pods(v1.NamespaceAll).DeleteCollection(&deleteOptions, listOptions)
	if err != nil {
		//TODO handle error
	}
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}

func createPodDeletionListOptions() metav1.ListOptions {
	markedForDeletionJobLabels := make([]string, 2)
	//Only delete batch jobs
	markedForDeletionJobLabels = append(markedForDeletionJobLabels, domain.JobId)
	//Only delete jobs with ready for cleanup = true
	markedForDeletionJobLabels = append(markedForDeletionJobLabels, domain.ReadyForCleanup+"="+strconv.FormatBool(true))

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(markedForDeletionJobLabels, ","),
	}

	return listOptions
}
