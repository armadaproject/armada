package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

var managedPodSelector labels.Selector

func init() {
	managedPodSelector = createLabelSelectorForManagedPods()
}

func IsInTerminalState(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	if podPhase == v1.PodSucceeded || podPhase == v1.PodFailed {
		return true
	}
	return false
}

func IsManagedPod(pod *v1.Pod) bool {
	if _, ok := pod.Labels[domain.JobId]; !ok {
		return false
	}

	return true
}

func GetManagedPodSelector() labels.Selector {
	return managedPodSelector.DeepCopySelector()
}

func createLabelSelectorForManagedPods() labels.Selector {
	jobIdExistsRequirement, err := labels.NewRequirement(domain.JobId, selection.Exists, []string{})
	if err != nil {
		panic(err)
	}

	selector := labels.NewSelector().Add(*jobIdExistsRequirement)
	return selector
}

func ExtractJobIds(pods []*v1.Pod) []string {
	jobIds := make([]string, 0, len(pods))

	for _, pod := range pods {
		if jobId, ok := pod.Labels[domain.JobId]; ok {
			jobIds = append(jobIds, jobId)
		} else {
			//TODO decide how to handle this error, it should in theory never happen if all jobs are well formed. Maybe skipping is OK
			log.Errorf("Failed to report event for pod %s as no job id was present to report it under.", pod.Name)
		}
	}

	return jobIds
}

func FilterCompletedPods(pods []*v1.Pod) []*v1.Pod {
	completedPods := make([]*v1.Pod, 0, len(pods))

	for _, pod := range pods {
		if IsInTerminalState(pod) {
			completedPods = append(completedPods, pod)
		}
	}

	return completedPods
}

func FilterNonCompletedPods(pods []*v1.Pod) []*v1.Pod {
	activePods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if !IsInTerminalState(pod) {
			activePods = append(activePods, pod)
		}
	}

	return activePods
}
