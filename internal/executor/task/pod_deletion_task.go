package task

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"time"
)

type PodDeletionTask struct {
	KubernetesClient kubernetes.Interface
	Interval         time.Duration
}

func (deletionTask PodDeletionTask) Execute() {
	deleteOptions := createPodDeletionDeleteOptions()
	listOptions := service.CreateListOptionsForManagedPods(true)

	//TODO decide how to handle namespaces, or select from all namespaces and delete individually
	err := deletionTask.KubernetesClient.CoreV1().Pods("default").DeleteCollection(&deleteOptions, listOptions)
	if err != nil {
		fmt.Println(err)
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

func (deletionTask PodDeletionTask) GetInterval() time.Duration {
	return deletionTask.Interval
}
