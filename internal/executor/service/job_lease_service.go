package service

import (
	"context"
	"fmt"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	v1 "k8s.io/api/core/v1"
	listers "k8s.io/client-go/listers/core/v1"
	"strings"
	"time"
)

type JobLeaseService struct {
	PodLister      listers.PodLister
	QueueClient    api.AggregatedQueueClient
	CleanupService PodCleanupService
	ClusterId      string
}

func (jobLeaseService JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources) ([]*api.Job, error) {
	leaseRequest := api.LeaseRequest{
		ClusterID: jobLeaseService.ClusterId,
		Resources: *availableResource,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := jobLeaseService.QueueClient.LeaseJobs(ctx, &leaseRequest)

	if err != nil {
		return make([]*api.Job, 0), err
	}

	return response.Job, nil
}

func (jobLeaseService JobLeaseService) ManageJobLeases() {
	managedPodSelector := util.GetManagedPodSelector()
	allManagedPods, err := jobLeaseService.PodLister.List(managedPodSelector)
	if err != nil {
		//TODO Handle error case
	}

	podsToRenew, podsToCleanup := splitRunningAndFinishedPods(allManagedPods)

	jobLeaseService.renewJobLeases(podsToRenew)
	jobLeaseService.endJobLeases(podsToCleanup)
}

func splitRunningAndFinishedPods(pods []*v1.Pod) (running []*v1.Pod, finished []*v1.Pod) {
	runningPods := make([]*v1.Pod, 0)
	finishedPods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if IsPodReadyForCleanup(pod) {
			finishedPods = append(finishedPods, pod)
		} else {
			runningPods = append(runningPods, pod)
		}
	}

	return runningPods, finishedPods
}

func (jobLeaseService JobLeaseService) renewJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)
	fmt.Printf("Renewing lease for %s \n", strings.Join(jobIds, ","))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := jobLeaseService.QueueClient.RenewLease(ctx, &api.IdList{Ids: jobIds})

	if err != nil {
		fmt.Printf("Failed to renew lease for jobs because %s \n", err)
	}
}

func (jobLeaseService JobLeaseService) endJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := jobLeaseService.QueueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	if err != nil {
		fmt.Printf("Failed cleaning up jobs because %s \n", err)
	}

	jobLeaseService.CleanupService.DeletePods(pods)
}
