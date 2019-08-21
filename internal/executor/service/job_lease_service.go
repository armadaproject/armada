package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	commonUtil "github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	log "github.com/sirupsen/logrus"
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
		log.Errorf("Failed to manage job leases due to %s", err)
		return
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
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	renewedJobIds, err := jobLeaseService.QueueClient.RenewLease(ctx, &api.RenewLeaseRequest{ClusterID: jobLeaseService.ClusterId, Ids: jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	for _, jobId := range failedIds {
		log.WithField("jobId", jobId).Error("Failed to renew job lease")
		// TODO: kill the pods ???
	}

}

func (jobLeaseService JobLeaseService) endJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	reported, err := jobLeaseService.QueueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	if err != nil {
		log.Errorf("Failed cleaning up jobs because %s", err)
		return
	}

	reportedIdSet := commonUtil.StringListToSet(reported.Ids)
	podsToDelete := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if reportedIdSet[util.ExtractJobId(pod)] {
			podsToDelete = append(podsToDelete, pod)
		}
	}
	jobLeaseService.CleanupService.DeletePods(podsToDelete)
}
