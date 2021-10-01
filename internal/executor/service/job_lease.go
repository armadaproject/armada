package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	log "github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"

	"github.com/G-Research/armada/internal/common"
	commonUtil "github.com/G-Research/armada/internal/common/util"
	context2 "github.com/G-Research/armada/internal/executor/context"
	"github.com/G-Research/armada/internal/executor/job"
	"github.com/G-Research/armada/internal/executor/util"
	"github.com/G-Research/armada/pkg/api"
)

const maxPodRequestSize = 10000

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error)
	RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error)
	ReportDone(jobIds []string) error
}

type JobLeaseService struct {
	clusterContext         context2.ClusterContext
	queueClient            api.AggregatedQueueClient
	minimumJobSize         common.ComputeResources
	avoidNodeLabelsOnRetry []string
}

func NewJobLeaseService(
	clusterContext context2.ClusterContext,
	queueClient api.AggregatedQueueClient,
	minimumJobSize common.ComputeResources,
	avoidNodeLabelsOnRetry []string) *JobLeaseService {

	return &JobLeaseService{
		clusterContext:         clusterContext,
		queueClient:            queueClient,
		minimumJobSize:         minimumJobSize,
		avoidNodeLabelsOnRetry: avoidNodeLabelsOnRetry,
	}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource *common.ComputeResources, nodes []api.NodeInfo, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	leasedQueueReports := make([]*api.QueueLeasedReport, 0, len(leasedResourceByQueue))
	for queueName, leasedResource := range leasedResourceByQueue {
		leasedQueueReport := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: leasedResource,
		}
		leasedQueueReports = append(leasedQueueReports, leasedQueueReport)
	}
	clusterLeasedReport := api.ClusterLeasedReport{
		ClusterId:  jobLeaseService.clusterContext.GetClusterId(),
		ReportTime: time.Now(),
		Queues:     leasedQueueReports,
	}

	leaseRequest := api.LeaseRequest{
		ClusterId:           jobLeaseService.clusterContext.GetClusterId(),
		Pool:                jobLeaseService.clusterContext.GetClusterPool(),
		Resources:           *availableResource,
		ClusterLeasedReport: clusterLeasedReport,
		Nodes:               nodes,
		MinimumJobSize:      jobLeaseService.minimumJobSize,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	response, err := jobLeaseService.queueClient.LeaseJobs(ctx, &leaseRequest, grpc_retry.WithMax(1))

	if err != nil {
		return make([]*api.Job, 0), err
	}

	return response.Job, nil
}

func (jobLeaseService *JobLeaseService) ReturnLease(pod *v1.Pod) error {
	jobId := util.ExtractJobId(pod)
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()

	avoidNodeLabels, err := jobLeaseService.getAvoidNodeLabels(pod)
	if err != nil {
		log.Warnf("Failed to get node labels to avoid on rerun for pod %s: %v", pod.Name, err)
		avoidNodeLabels = map[string]string{}
	}

	log.Infof("Returning lease for job %s (will try to avoid these node labels next time: %v)", jobId, avoidNodeLabels)
	_, err = jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterContext.GetClusterId(), JobId: jobId, AvoidNodeLabels: avoidNodeLabels})
	return err
}

func (jobLeaseService *JobLeaseService) ReportDone(jobIds []string) error {
	if len(jobIds) <= 0 {
		return nil
	}
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	log.Infof("Reporting done for jobs %s", strings.Join(jobIds, ","))
	_, err := jobLeaseService.queueClient.ReportDone(ctx, &api.IdList{Ids: jobIds})

	return err
}

func (jobLeaseService *JobLeaseService) RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error) {
	if len(jobs) <= 0 {
		return []*job.RunningJob{}, nil
	}
	jobIds := extractJobIds(jobs)
	log.Infof("Renewing lease for %s", strings.Join(jobIds, ","))

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	renewedJobIds, err := jobLeaseService.queueClient.RenewLease(ctx,
		&api.RenewLeaseRequest{
			ClusterId: jobLeaseService.clusterContext.GetClusterId(),
			Ids:       jobIds})
	if err != nil {
		log.Errorf("Failed to renew lease for jobs because %s", err)
		return nil, err
	}

	failedIds := commonUtil.SubtractStringList(jobIds, renewedJobIds.Ids)
	failedJobs := filterRunningJobsByIds(jobs, failedIds)

	if len(failedIds) > 0 {
		log.Warnf("Server has prevented renewing of job lease for jobs %s", strings.Join(failedIds, ","))
	}

	return failedJobs, nil
}

func (jobLeaseService *JobLeaseService) getAvoidNodeLabels(pod *v1.Pod) (map[string]string, error) {

	if len(jobLeaseService.avoidNodeLabelsOnRetry) == 0 {
		return map[string]string{}, nil
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		return nil, errors.New("Empty node name in pod")
	}

	node, err := jobLeaseService.clusterContext.GetNode(nodeName)
	if err != nil {
		return nil, fmt.Errorf("Could not get node %s from Kubernetes api: %v", nodeName, err)
	}

	return commonUtil.FilterKeys(node.Labels, jobLeaseService.avoidNodeLabelsOnRetry), nil
}
