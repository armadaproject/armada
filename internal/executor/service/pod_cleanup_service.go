package service

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	lister "k8s.io/client-go/listers/core/v1"
)

type PodCleanupService struct {
	KubernetesClient kubernetes.Interface
	EventReporter    reporter.EventReporter
	PodLister        lister.PodLister
}

func (cleanupService PodCleanupService) DeletePodsReadyForCleanup() {
	deleteOptions := createPodDeletionDeleteOptions()

	podsReadyForDeletion := cleanupService.getAllPodsToBeDeleted()

	for _, podToDelete := range podsReadyForDeletion {
		err := cleanupService.KubernetesClient.CoreV1().Pods(podToDelete.Namespace).Delete(podToDelete.Name, &deleteOptions)
		if err != nil {
			fmt.Printf("Failed to delete pod %s/%s because %s", podToDelete.Namespace, podToDelete.Name, err)
		}
	}
}

func (cleanupService PodCleanupService) getAllPodsToBeDeleted() []*v1.Pod {
	selector, err := util.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		fmt.Println(err)
		return []*v1.Pod{}
	}

	allBatchPods, err := cleanupService.PodLister.List(selector)
	if err != nil {
		fmt.Println(err)
		return []*v1.Pod{}
	}

	podsReadyForDeletion := make([]*v1.Pod, 0)

	for _, pod := range allBatchPods {
		if util.IsInTerminalState(pod) && hasEventBeenReportedForCurrentState(pod) {
			podsReadyForDeletion = append(podsReadyForDeletion, pod)
		}
	}

	return podsReadyForDeletion
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}
