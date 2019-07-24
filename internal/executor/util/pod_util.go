package util

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

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

func CreateLabelSelectorForManagedPods() (labels.Selector, error) {
	jobIdExistsRequirement, err := labels.NewRequirement(domain.JobId, selection.Exists, []string{})
	if err != nil {
		return labels.NewSelector(), err
	}

	selector := labels.NewSelector().Add(*jobIdExistsRequirement)
	return selector, nil
}

func ExtractJobIds(pods []*v1.Pod) []string {
	jobIds := make([]string, 0, len(pods))

	for _, pod := range pods {
		jobId := pod.Labels[domain.JobId]
		jobIds = append(jobIds, jobId)
	}

	return jobIds
}
