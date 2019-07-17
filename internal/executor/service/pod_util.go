package service

import (
	"errors"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/model"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"strconv"
	"strings"
)

func IsInTerminalState(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	if podPhase == v1.PodSucceeded || podPhase == v1.PodFailed {
		return true
	}
	return false
}

func ExtractJobStatus(pod *v1.Pod) (model.JobStatus, error) {
	phase := pod.Status.Phase

	switch phase {
	case v1.PodPending:
		return model.Pending, nil
	case v1.PodRunning:
		return model.Running, nil
	case v1.PodSucceeded:
		return model.Succeeded, nil
	case v1.PodFailed:
		return model.Failed, nil
	default:
		return *new(model.JobStatus), errors.New(fmt.Sprintf("Could not determine job status from pod in phase %s", phase))
	}
}

func IsManagedPod(pod *v1.Pod) bool {
	if _, ok := pod.Labels[domain.JobId]; !ok {
		return false
	}

	return true
}

func CreateLabelSelectorForManagedPods(podsReadyForCleanup bool) (labels.Selector, error) {
	jobIdExistsRequirement, err := labels.NewRequirement(domain.JobId, selection.Exists, []string{})
	if err != nil {
		return labels.NewSelector(), err
	}

	readyForCleanupRequirement, err := labels.NewRequirement(domain.ReadyForCleanup, selection.Equals, []string{strconv.FormatBool(podsReadyForCleanup)})
	if err != nil {
		return labels.NewSelector(), err
	}

	selector := labels.NewSelector().Add(*jobIdExistsRequirement, *readyForCleanupRequirement)
	return selector, nil
}

func CreateListOptionsForManagedPods(podsReadyForCleanup bool) metav1.ListOptions {
	managedPodLabels := make([]string, 0, 2)
	managedPodLabels = append(managedPodLabels, domain.JobId)
	managedPodLabels = append(managedPodLabels, domain.ReadyForCleanup+"="+strconv.FormatBool(podsReadyForCleanup))

	listOptions := metav1.ListOptions{
		LabelSelector: strings.Join(managedPodLabels, ","),
	}

	return listOptions
}
