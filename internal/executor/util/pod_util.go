package util

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
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

func ExtractNames(pods []*v1.Pod) []string {
	podNames := make([]string, 0, len(pods))

	for _, pod := range pods {
		podNames = append(podNames, pod.Name)
	}

	return podNames
}

func ExtractJobIds(pods []*v1.Pod) []string {
	jobIds := make([]string, 0, len(pods))

	for _, pod := range pods {
		if jobId, ok := pod.Labels[domain.JobId]; ok {
			jobIds = append(jobIds, jobId)
		}
	}

	return jobIds
}

func ExtractJobId(pod *v1.Pod) string {
	return pod.Labels[domain.JobId]
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

func FilterPodsWithPhase(pods []*v1.Pod, podPhase v1.PodPhase) []*v1.Pod {
	podsInPhase := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if pod.Status.Phase == podPhase {
			podsInPhase = append(podsInPhase, pod)
		}
	}

	return podsInPhase
}

func IsReportingPhaseRequired(podPhase v1.PodPhase) bool {
	return podPhase != v1.PodPending && podPhase != v1.PodUnknown
}

func MergePodList(list1 []*v1.Pod, list2 []*v1.Pod) []*v1.Pod {
	jobIds := ExtractNames(list1)
	jobIdsSet := util.StringListToSet(jobIds)

	allPods := list1

	for _, pod := range list2 {
		if !jobIdsSet[pod.Name] {
			allPods = append(allPods, pod)
		}
	}

	return allPods
}
