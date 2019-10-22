package service

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/armada/api"
	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
)

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource *common.ComputeResources) ([]*api.Job, error)
	ReportDone(pods []*v1.Pod) error
}

type JobLeaseService struct {
	clusterContext context2.ClusterContext
	queueClient    api.AggregatedQueueClient
}

func NewJobLeaseService(
	clusterContext context2.ClusterContext,
	queueClient api.AggregatedQueueClient) *JobLeaseService {

	return &JobLeaseService{
		clusterContext: clusterContext,
		queueClient:    queueClient}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources) ([]*api.Job, error) {
	leaseRequest := api.LeaseRequest{
		ClusterId: jobLeaseService.clusterContext.GetClusterId(),
		Resources: *availableResource,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := jobLeaseService.queueClient.LeaseJobs(ctx, &leaseRequest)

	if err != nil {
		return make([]*api.Job, 0), err
	}

	return response.Job, nil
}

func (jobLeaseService *JobLeaseService) ReturnLease(pod *v1.Pod) error {
	jobId := util.ExtractJobId(pod)
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Returning lease for job %s", jobId)
	_, err := jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterContext.GetClusterId(), JobId: jobId})

	return err
}

func (jobLeaseService *JobLeaseService) ManageJobLeases() {
	podsToRenew, err := jobLeaseService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to manage job leases due to %s", err)
		return
	}

	podsToCleanup := getFinishedPods(podsToRenew)

	jobLeaseService.renewJobLeases(podsToRenew)

	err = jobLeaseService.ReportDone(podsToCleanup)
	if err != nil {
		log.Errorf("Failed reporting jobs as done because %s", err)
		return
	}

	jobLeaseService.clusterContext.DeletePods(podsToCleanup)
}

func (jobLeaseService *JobLeaseService) ReportDone(pods []*v1.Pod) error {
	if len(pods) <= 0 {
		return nil
	}
	jobIds := util.ExtractJobIds(pods)

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Reporting done for jobs %s", strings.Join(jobIds, ","))
	_, err := jobLeaseService.queueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	return err
}

func (jobLeaseService *JobLeaseService) renewJobLeases(pods []*v1.Pod) {
	if len(pods) <= 0 {
		return
	}
	jobIds := util.ExtractJobIds(pods)
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	renewedJobIds, err := jobLeaseService.queueClient.RenewLease(ctx,
		&api.RenewLeaseRequest{
			ClusterId: jobLeaseService.clusterContext.GetClusterId(),
			Ids:       jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	failedPods := filterPodsByJobId(pods, failedIds)
	if len(failedIds) > 0 {
		log.Warnf("Server has prevented renewing of job lease for jobs %s", strings.Join(failedIds, ","))
		jobLeaseService.clusterContext.DeletePods(failedPods)
	}
}

func getFinishedPods(pods []*v1.Pod) []*v1.Pod {
	finishedPods := make([]*v1.Pod, 0)

	for _, pod := range pods {
		if isPodReadyForCleanup(pod) {
			finishedPods = append(finishedPods, pod)
		}
	}

	return finishedPods
}

func filterPodsByJobId(pods []*v1.Pod, ids []string) []*v1.Pod {
	reportedIdSet := commonUtil.StringListToSet(ids)
	filteredPods := make([]*v1.Pod, 0)
	for _, pod := range pods {
		if reportedIdSet[util.ExtractJobId(pod)] {
			filteredPods = append(filteredPods, pod)
		}
	}
	return filteredPods
}

func isPodReadyForCleanup(pod *v1.Pod) bool {
	if util.IsInTerminalState(pod) && reporter.HasCurrentStateBeenReported(pod) {
		return true
	}
	return false
}
