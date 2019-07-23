package service

import (
	"errors"
	"github.com/G-Research/k8s-batch/internal/executor/reporter"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	listers "k8s.io/client-go/listers/core/v1"
	"time"
)

type JobEventReconciliationService struct {
	PodLister     listers.PodLister
	EventReporter reporter.EventReporter
}

func (reconciliationService JobEventReconciliationService) ReconcileMissingJobEvents() {
	selector, err := util.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		return
		//TODO Handle error case
	}

	allBatchPodsNotMarkedForCleanup, err := reconciliationService.PodLister.List(selector)
	allPodsWithMissingEvent := filterPodsWithCurrentStateNotReported(allBatchPodsNotMarkedForCleanup)

	for _, pod := range allPodsWithMissingEvent {
		go reconciliationService.EventReporter.ReportEvent(pod)
	}
}

func filterPodsWithCurrentStateNotReported(pods []*v1.Pod) []*v1.Pod {
	podsWithMissingEvent := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if !hasEventBeenReportedForCurrentState(pod) {
			if hasPodBeenInStateForLongerThanGivenDuration(pod, 15*time.Second) {
				podsWithMissingEvent = append(podsWithMissingEvent, pod)
			}
		}
	}
	return podsWithMissingEvent
}

func hasEventBeenReportedForCurrentState(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	_, annotationPresent := pod.Annotations[string(podPhase)]
	return annotationPresent
}

func hasPodBeenInStateForLongerThanGivenDuration(pod *v1.Pod, duration time.Duration) bool {
	deadline := time.Now().Add(-duration)
	lastStatusChange, err := lastStatusChange(pod)

	if err != nil || lastStatusChange.Before(deadline) {
		return true
	}
	return false
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
