package service

import (
	"context"
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
const jobDoneAnnotation = "reported_done"

type LeaseService interface {
	ReturnLease(pod *v1.Pod) error
	RequestJobLeases(availableResource common.ComputeResources, nodes []api.NodeInfo, autoscalingPools []api.AutoscalingPool, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error)
	RenewJobLeases(jobs []*job.RunningJob) ([]*job.RunningJob, error)
	ReportDone(jobIds []string) error
}

type JobLeaseService struct {
	clusterIdentity context2.ClusterIdentity
	queueClient     api.AggregatedQueueClient
	minimumJobSize  common.ComputeResources
}

func NewJobLeaseService(
	clusterIdentity context2.ClusterIdentity,
	queueClient api.AggregatedQueueClient,
	minimumJobSize common.ComputeResources) *JobLeaseService {

	return &JobLeaseService{
		clusterIdentity: clusterIdentity,
		queueClient:     queueClient,
		minimumJobSize:  minimumJobSize,
	}
}

func (jobLeaseService *JobLeaseService) RequestJobLeases(availableResource common.ComputeResources, nodes []api.NodeInfo, autoscalingPools []api.AutoscalingPool, leasedResourceByQueue map[string]common.ComputeResources) ([]*api.Job, error) {
	leasedQueueReports := make([]*api.QueueLeasedReport, 0, len(leasedResourceByQueue))
	for queueName, leasedResource := range leasedResourceByQueue {
		leasedQueueReport := &api.QueueLeasedReport{
			Name:            queueName,
			ResourcesLeased: leasedResource,
		}
		leasedQueueReports = append(leasedQueueReports, leasedQueueReport)
	}
	clusterLeasedReport := api.ClusterLeasedReport{
		ClusterId:  jobLeaseService.clusterIdentity.GetClusterId(),
		ReportTime: time.Now(),
		Queues:     leasedQueueReports,
	}

	leaseRequest := api.LeaseRequest{
		ClusterId:           jobLeaseService.clusterIdentity.GetClusterId(),
		Pool:                jobLeaseService.clusterIdentity.GetClusterPool(),
		Resources:           availableResource,
		AutoscalingPools:    autoscalingPools,
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
	log.Infof("Returning lease for job %s", jobId)
	_, err := jobLeaseService.queueClient.ReturnLease(ctx, &api.ReturnLeaseRequest{ClusterId: jobLeaseService.clusterIdentity.GetClusterId(), JobId: jobId})

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
			ClusterId: jobLeaseService.clusterIdentity.GetClusterId(),
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
