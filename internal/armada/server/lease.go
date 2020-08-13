package server

import (
	"context"

	"github.com/gogo/protobuf/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/api"
)

type AggregatedQueueServer struct {
	permissions              authorization.PermissionChecker
	schedulingConfig         configuration.SchedulingConfig
	jobRepository            repository.JobRepository
	queueRepository          repository.QueueRepository
	usageRepository          repository.UsageRepository
	eventStore               repository.EventStore
	schedulingInfoRepository repository.SchedulingInfoRepository
}

func NewAggregatedQueueServer(
	permissions authorization.PermissionChecker,
	schedulingConfig configuration.SchedulingConfig,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	usageRepository repository.UsageRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
) *AggregatedQueueServer {
	return &AggregatedQueueServer{
		permissions:              permissions,
		schedulingConfig:         schedulingConfig,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		usageRepository:          usageRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository}
}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return &api.JobLease{}, nil
	}

	queues, e := q.queueRepository.GetAllQueues()
	if e != nil {
		return nil, e
	}

	activeQueues, e := q.jobRepository.FilterActiveQueues(queues)
	if e != nil {
		return nil, e
	}

	usageReports, e := q.usageRepository.GetClusterUsageReports()
	if e != nil {
		return nil, e
	}

	e = q.usageRepository.UpdateClusterLeased(&request.ClusterLeasedReport)
	if e != nil {
		return nil, e
	}

	nodeResources := scheduling.AggregateNodeTypeAllocations(request.Nodes)
	clusterSchedulingInfo := scheduling.CreateClusterSchedulingInfoReport(request, nodeResources)
	e = q.schedulingInfoRepository.UpdateClusterSchedulingInfo(clusterSchedulingInfo)
	if e != nil {
		return nil, e
	}

	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	clusterPriorities, e := q.usageRepository.GetClusterPriorities(scheduling.GetClusterReportIds(activeClusterReports))
	if e != nil {
		return nil, e
	}

	clusterLeasedJobReports, e := q.usageRepository.GetClusterLeasedReports()
	if e != nil {
		return nil, e
	}
	clusterLeasedJobReports = scheduling.FilterActiveClusterLeasedReports(clusterLeasedJobReports)

	jobs, e := scheduling.LeaseJobs(
		ctx,
		&q.schedulingConfig,
		q.jobRepository,
		func(jobs []*api.Job) { reportJobsLeased(q.eventStore, jobs, request.ClusterId) },
		request,
		nodeResources,
		activeClusterReports,
		clusterLeasedJobReports,
		clusterPriorities,
		activeQueues)

	if e != nil {
		return nil, e
	}

	clusterLeasedReport := scheduling.CreateClusterLeasedReport(request.ClusterLeasedReport.ClusterId, &request.ClusterLeasedReport, jobs)
	e = q.usageRepository.UpdateClusterLeased(clusterLeasedReport)
	if e != nil {
		return nil, e
	}

	jobLease := api.JobLease{
		Job: jobs,
	}
	return &jobLease, nil
}

func (q *AggregatedQueueServer) RenewLease(ctx context.Context, request *api.RenewLeaseRequest) (*api.IdList, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	renewed, e := q.jobRepository.RenewLease(request.ClusterId, request.Ids)
	return &api.IdList{renewed}, e
}

func (q *AggregatedQueueServer) ReturnLease(ctx context.Context, request *api.ReturnLeaseRequest) (*types.Empty, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	_, err := q.jobRepository.ReturnLease(request.ClusterId, request.JobId)
	if err != nil {
		return nil, err
	}
	return &types.Empty{}, nil
}

func (q *AggregatedQueueServer) ReportDone(ctx context.Context, idList *api.IdList) (*api.IdList, error) {
	if e := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); e != nil {
		return nil, e
	}
	jobs, e := q.jobRepository.GetExistingJobsByIds(idList.Ids)
	if e != nil {
		return nil, status.Errorf(codes.Internal, e.Error())
	}
	deletionResult := q.jobRepository.DeleteJobs(jobs)

	cleanedIds := make([]string, 0, len(deletionResult))
	var returnedError error = nil
	for job, err := range deletionResult {
		if err != nil {
			returnedError = err
		} else {
			cleanedIds = append(cleanedIds, job.Id)
		}
	}
	return &api.IdList{cleanedIds}, returnedError
}
