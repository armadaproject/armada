package util

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"

	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/executor/domain"
)

var managedPodSelector labels.Selector

func init() {
	managedPodSelector = createLabelSelectorForManagedPods()
}

func HasIngress(pod *v1.Pod) bool {
	value, exists := pod.Annotations[domain.HasIngress]
	return exists && value == "true"
}

func IsInTerminalState(pod *v1.Pod) bool {
	podPhase := pod.Status.Phase
	if podPhase == v1.PodSucceeded || podPhase == v1.PodFailed {
		return true
	}
	return false
}

func IsManagedPod(pod *v1.Pod) bool {
	_, ok := pod.Labels[domain.JobId]
	return ok
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
		if jobId := ExtractJobId(pod); jobId != "" {
			jobIds = append(jobIds, jobId)
		}
	}

	return jobIds
}

func ExtractJobId(pod *v1.Pod) string {
	return pod.Labels[domain.JobId]
}

func ExtractPodNumber(pod *v1.Pod) int {
	i, _ := strconv.Atoi(pod.Labels[domain.PodNumber])
	return i
}

func ExtractPodKey(pod *v1.Pod) string {
	return fmt.Sprintf("%s_%d", ExtractJobId(pod), ExtractPodNumber(pod))
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

func FilterPods(pods []*v1.Pod, filter func(*v1.Pod) bool) []*v1.Pod {
	result := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if filter(pod) {
			result = append(result, pod)
		}
	}
	return result
}

func LastStatusChange(pod *v1.Pod) (time.Time, error) {
	maxStatusChange := pod.CreationTimestamp.Time
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if s := containerStatus.State.Running; s != nil {
			maxStatusChange = maxTime(maxStatusChange, s.StartedAt.Time)
		}
		if s := containerStatus.State.Terminated; s != nil {
			maxStatusChange = maxTime(maxStatusChange, s.FinishedAt.Time)
		}
	}

	for _, condition := range pod.Status.Conditions {
		maxStatusChange = maxTime(maxStatusChange, condition.LastTransitionTime.Time)
	}

	if maxStatusChange.IsZero() {
		return maxStatusChange, errors.New("cannot determine last status change")
	}
	return maxStatusChange, nil
}

func FindLastContainerStartTime(pod *v1.Pod) time.Time {
	//Fallback to pod creation if there is no container
	startTime := pod.CreationTimestamp.Time
	for _, c := range pod.Status.ContainerStatuses {
		if s := c.State.Running; s != nil {
			startTime = maxTime(startTime, s.StartedAt.Time)
		}
		if s := c.State.Terminated; s != nil {
			startTime = maxTime(startTime, s.StartedAt.Time)
		}
	}
	return startTime
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

func GetPodContainerStatuses(pod *v1.Pod) []v1.ContainerStatus {
	containerStatuses := pod.Status.ContainerStatuses
	containerStatuses = append(containerStatuses, pod.Status.InitContainerStatuses...)
	return containerStatuses
}
