package service

import (
	"github.com/G-Research/k8s-batch/internal/armada/api"
	"github.com/G-Research/k8s-batch/internal/common"
	commonUtil "github.com/G-Research/k8s-batch/internal/common/util"
	"github.com/G-Research/k8s-batch/internal/executor/util"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	listers "k8s.io/client-go/listers/core/v1"
	"strings"
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
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	response, err := jobLeaseService.QueueClient.LeaseJobs(ctx, &leaseRequest)

	if err != nil {
		return make([]*api.Job, 0), err
	}

	return response.Job, nil
}

func (jobLeaseService JobLeaseService) RenewJobLeases() {
	allManagedPods, err := jobLeaseService.PodLister.List(util.GetManagedPodSelector())
	if err != nil {
		log.Errorf("Failed to renew job leases due to %s", err)
		return
	}

	podsToRenew := getRunningPods(allManagedPods)
	jobLeaseService.renewJobLeases(podsToRenew)
}

func (jobLeaseService JobLeaseService) CleanupJobLeases() {
	allManagedPods, err := jobLeaseService.PodLister.List(util.GetManagedPodSelector())
	if err != nil {
		log.Errorf("Failed to cleanp job leases due to %s", err)
		return
	}

	podsToCleanup := getFinishedPods(allManagedPods)
	jobLeaseService.cleanupJobLeases(podsToCleanup)
}

func (jobLeaseService JobLeaseService) renewJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	renewedJobIds, err := jobLeaseService.QueueClient.RenewLease(ctx, &api.RenewLeaseRequest{ClusterID: jobLeaseService.ClusterId, Ids: jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	log.Errorf("Failed to renew job lease for jobs %s", strings.Join(failedIds, ","))
	jobLeaseService.deletePodsWithIds(pods, failedIds)
}

func (jobLeaseService JobLeaseService) cleanupJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	reported, err := jobLeaseService.QueueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	if err != nil {
		log.Errorf("Failed reporting jobs as done because %s", err)
		return
	}

	jobLeaseService.deletePodsWithIds(pods, reported.Ids)
}

func getRunningPods(pods []*v1.Pod) []*v1.Pod {
	runningPods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if !IsPodReadyForCleanup(pod) {
			runningPods = append(runningPods, pod)
		}
	}

	return runningPods
}

func getFinishedPods(pods []*v1.Pod) []*v1.Pod {
	finishedPods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if IsPodReadyForCleanup(pod) {
			finishedPods = append(finishedPods, pod)
		}
	}

	return finishedPods
}

func (jobLeaseService JobLeaseService) deletePodsWithIds(pods []*v1.Pod, idsToDelete []string) {
	reportedIdSet := commonUtil.StringListToSet(idsToDelete)
	podsToDelete := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if reportedIdSet[util.ExtractJobId(pod)] {
			podsToDelete = append(podsToDelete, pod)
		}
	}
	jobLeaseService.CleanupService.DeletePods(podsToDelete)
}
