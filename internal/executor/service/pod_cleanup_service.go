package service

import (
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	lister "k8s.io/client-go/listers/core/v1"
	"time"
)

type PodCleanupService struct {
	KubernetesClient kubernetes.Interface
	EventReporter    reporter.EventReporter
	PodLister        lister.PodLister
}

func (cleanupService PodCleanupService) DeletePodsReadyForCleanup() {
	deleteOptions := createPodDeletionDeleteOptions()
	listOptions := util.CreateListOptionsForManagedPods(true)

	namespacesToDeleteFrom := cleanupService.getAllNamespacesContainingPodsToBeDeleted()

	for _, namespace := range namespacesToDeleteFrom {
		err := cleanupService.KubernetesClient.CoreV1().Pods(namespace).DeleteCollection(&deleteOptions, listOptions)
		if err != nil {
			fmt.Println(err)
			//TODO handle error
		}
	}
}

func (cleanupService PodCleanupService) getAllNamespacesContainingPodsToBeDeleted() []string {
	selector, err := util.CreateLabelSelectorForManagedPods(true)
	if err != nil {
		fmt.Println(err)
		return []string{}
	}

	podsReadyToDelete, err := cleanupService.PodLister.List(selector)
	if err != nil {
		fmt.Println(err)
		return []string{}
	}

	allUniqueNamespaces := make(map[string]bool)

	for _, pod := range podsReadyToDelete {
		allUniqueNamespaces[pod.Namespace] = true
	}

	return keys(allUniqueNamespaces)
}

func keys(input map[string]bool) []string {
	keys := make([]string, 0, len(input))

	for key := range input {
		keys = append(keys, key)
	}

	return keys
}

func (cleanupService PodCleanupService) ReportForgottenCompletedPods() {
	podsToBeReported := getAllPodsRequiringCompletionEvent(cleanupService.PodLister)

	for _, pod := range podsToBeReported {
		cleanupService.EventReporter.ReportCompletedEvent(pod)
	}
}

func createPodDeletionDeleteOptions() metav1.DeleteOptions {
	gracePeriod := int64(0)
	deleteOptions := metav1.DeleteOptions{
		GracePeriodSeconds: &gracePeriod,
	}
	return deleteOptions
}

func getAllPodsRequiringCompletionEvent(podLister lister.PodLister) []*v1.Pod {
	selector, err := util.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		return nil
		//TODO Handle error case
	}

	allBatchPodsNotMarkedForCleanup, err := podLister.List(selector)

	if err != nil {
		//TODO Do something in case of error
	}

	completedBatchPodsNotMarkedForCleanup := filterCompletedPods(allBatchPodsNotMarkedForCleanup)
	completedBatchPodsToBeReported := filterPodsInStateForLongerThanGivenDuration(completedBatchPodsNotMarkedForCleanup, 5*time.Minute)

	return completedBatchPodsToBeReported
}

func filterCompletedPods(pods []*v1.Pod) []*v1.Pod {
	completedPods := make([]*v1.Pod, 0, len(pods))

	for _, pod := range pods {
		if util.IsInTerminalState(pod) {
			completedPods = append(completedPods, pod)
		}
	}

	return completedPods
}

func filterPodsInStateForLongerThanGivenDuration(pods []*v1.Pod, duration time.Duration) []*v1.Pod {
	podsInStateForLongerThanDuration := make([]*v1.Pod, 0)

	expiryTime := time.Now().Add(-duration)
	for _, pod := range pods {
		lastStatusChange, err := lastStatusChange(pod)
		if err != nil || lastStatusChange.Before(expiryTime) {
			podsInStateForLongerThanDuration = append(podsInStateForLongerThanDuration, pod)
		}
	}

	return podsInStateForLongerThanDuration
}

func lastStatusChange(pod *v1.Pod) (time.Time, error) {
	conditions := pod.Status.Conditions

	if len(conditions) <= 0 {
		return *new(time.Time), errors.New("no state changes found, cannot determine last status change")
	}

	var maxStatusChange time.Time

	for _, condition := range conditions {
		if condition.LastTransitionTime.Time.After(maxStatusChange) {
			maxStatusChange = condition.LastTransitionTime.Time
		}
	}

	return maxStatusChange, nil
}
