package service

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type PodCleanupService struct {
	KubernetesClient kubernetes.Interface
}

func (cleanupService PodCleanupService) DeletePods(pods []*v1.Pod) {
	deleteOptions := createPodDeletionDeleteOptions()

	for _, podToDelete := range pods {
		err := cleanupService.KubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(podToDelete.Name, &deleteOptions)
		if err != nil {
			fmt.Printf("Failed to delete pod %s/%s because %s", podToDelete.Namespace, podToDelete.Name, err)
		}
	}
}

func IsPodReadyForCleanup(pod *v1.Pod) bool {
	if util.IsInTerminalState(pod) && hasCurrentStateBeenReported(pod) {
		return true
	}
	return false
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}
