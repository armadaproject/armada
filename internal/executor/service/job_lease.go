package service

import (
	"context"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/reporter"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

const maxPodRequestSize = 10000
const jobDoneAnnotation = "reported_done"

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource *common.ComputeResources, availableLabels []map[string]string) ([]*api.Job, error)
	ReportDone(pods []*v1.Pod) error
}

type JobLeaseService struct {
	clusterContext  context2.ClusterContext
	queueClient     api.AggregatedQueueClient
	minimumPodAge   time.Duration
	failedPodExpiry time.Duration
}

func NewJobLeaseService(
	clusterContext context2.ClusterContext,
	queueClient api.AggregatedQueueClient,
	minimumPodAge time.Duration,
	failedPodExpiry time.Duration) *JobLeaseService {

	return &JobLeaseService{
		clusterContext:  clusterContext,
		queueClient:     queueClient,
		minimumPodAge:   minimumPodAge,
		failedPodExpiry: failedPodExpiry}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources, availableLabels []map[string]string) ([]*api.Job, error) {
	labeling := []*api.NodeLabeling{}
	for _, l := range availableLabels {
		labeling = append(labeling, &api.NodeLabeling{Labels: l})
	}

	leaseRequest := api.LeaseRequest{
		ClusterId:       jobLeaseService.clusterContext.GetClusterId(),
		Resources:       *availableResource,
		AvailableLabels: labeling,
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
	batchPods, err := jobLeaseService.clusterContext.GetBatchPods()
	if err != nil {
		log.Errorf("Failed to manage job leases due to %s", err)
		return
	}

	podsToRenew := util.FilterPods(batchPods, shouldBeRenewed)
	chunkedPods := chunkPods(podsToRenew, maxPodRequestSize)
	for _, chunk := range chunkedPods {
		jobLeaseService.renewJobLeases(chunk)
	}

	podsForReporting := util.FilterPods(batchPods, shouldBeReportedDone)
	chunkedPodsToReportDone := chunkPods(podsForReporting, maxPodRequestSize)
	for _, chunk := range chunkedPodsToReportDone {
		err = jobLeaseService.ReportDone(chunk)
		if err != nil {
			log.Errorf("Failed reporting jobs as done because %s", err)
			return
		}
	}

	podsToCleanup := util.FilterPods(batchPods, jobLeaseService.canBeRemoved)
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

	jobLeaseService.markAsDone(pods)
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

func (jobLeaseService *JobLeaseService) markAsDone(pods []*v1.Pod) {
	for _, pod := range pods {
		err := jobLeaseService.clusterContext.AddAnnotation(pod, map[string]string{
			jobDoneAnnotation: time.Now().String(),
		})
		if err != nil {
			log.Warnf("Failed to annotate pod as done: %v", err)
		}
	}
}

func shouldBeRenewed(pod *v1.Pod) bool {
	return !isReportedDone(pod)
}

func shouldBeReportedDone(pod *v1.Pod) bool {
	return util.IsInTerminalState(pod) && !isReportedDone(pod)
}

func (jobLeaseService *JobLeaseService) canBeRemoved(pod *v1.Pod) bool {
	if !util.IsInTerminalState(pod) ||
		!isReportedDone(pod) ||
		!reporter.HasCurrentStateBeenReported(pod) {
		return false
	}

	lastContainerStart := util.FindLastContainerStartTime(pod)
	if lastContainerStart.Add(jobLeaseService.minimumPodAge).After(time.Now()) {
		return false
	}

	if pod.Status.Phase == v1.PodFailed {
		lastChange, err := util.LastStatusChange(pod)
		if err == nil && lastChange.Add(jobLeaseService.failedPodExpiry).After(time.Now()) {
			return false
		}
	}
	return true
}

func isReportedDone(pod *v1.Pod) bool {
	_, exists := pod.Annotations[jobDoneAnnotation]
	return exists
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

func chunkPods(pods []*v1.Pod, size int) [][]*v1.Pod {
	chunks := [][]*v1.Pod{}
	for start := 0; start < len(pods); start += size {
		end := start + size
		if end > len(pods) {
			end = len(pods)
		}
		chunks = append(chunks, pods[start:end])
	}
	return chunks
}
