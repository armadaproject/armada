package task

import (
	"fmt"
	"github.com/G-Research/k8s-batch/internal/executor/domain"
	"github.com/G-Research/k8s-batch/internal/executor/service"
	v1 "k8s.io/api/core/v1"
	lister "k8s.io/client-go/listers/core/v1"
	"strings"
	"time"
)

type JobLeaseRenewalTask struct {
	PodLister lister.PodLister
	Interval  time.Duration
	//TODO API
}

func (jobLeaseRenewal JobLeaseRenewalTask) Execute() {
	runningPodsSelector, err := service.CreateLabelSelectorForManagedPods(false)
	if err != nil {
		//TODO Handle error case
	}

	allPodsEligibleForRenewal, err := jobLeaseRenewal.PodLister.List(runningPodsSelector)
	if err != nil {
		//TODO Handle error case
	}
	if len(allPodsEligibleForRenewal) > 0 {
		jobIds := extractJobIds(allPodsEligibleForRenewal)
		fmt.Printf("Renewing lease for %s \n", strings.Join(jobIds, ","))
	}
}

func extractJobIds(pods []*v1.Pod) []string {
	jobIds := make([]string, 0, len(pods))

	for _, pod := range pods {
		jobId := pod.Labels[domain.JobId]
		jobIds = append(jobIds, jobId)
	}

	return jobIds
}

func (jobLeaseRenewal JobLeaseRenewalTask) GetInterval() time.Duration {
	return jobLeaseRenewal.Interval
}
