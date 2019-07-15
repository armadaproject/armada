package task

import (
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	lister "k8s.io/client-go/listers/core/v1"
	"strconv"
)

type JobLeaseRenewalTask struct {
	PodLister lister.PodLister
	//TODO API
}

func (jobLeaseRenewal JobLeaseRenewalTask) Run() {
	runningPodsSelector, err := createRunningPodLabelSelector()
	if err != nil {
		//TODO Handle error case
	}

	allPodsEligibleForRenewal, err := jobLeaseRenewal.PodLister.List(runningPodsSelector)
	if err != nil {
		//TODO Handle error case
	}
	if len(allPodsEligibleForRenewal) > 0 {
		//extractJobIds(allPodsEligibleForRenewal)
	}
}

func createRunningPodLabelSelector() (labels.Selector, error) {
	jobIdExistsRequirement, err := labels.NewRequirement(domain.JobId, selection.Exists, []string{})
	if err != nil {
		return labels.NewSelector(), err
		//TODO Handle error case
	}

	notReadyCleanupRequirement, err := labels.NewRequirement(domain.ReadyForCleanup, selection.Equals, []string{strconv.FormatBool(false)})
	if err != nil {
		return labels.NewSelector(), err
		//TODO Handle error case
	}

	selector := labels.NewSelector().Add(*jobIdExistsRequirement, *notReadyCleanupRequirement)
	return selector, nil
}

func extractJobIds(pods []*v1.Pod) []string {
	jobIds := make([]string, 0, len(pods))

	for _, pod := range pods {
		jobId := pod.Labels[domain.JobId]
		jobIds = append(jobIds, jobId)
	}

	return jobIds
}
