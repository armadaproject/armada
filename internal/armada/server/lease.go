package server

import (
	"context"
	"errors"
	"fmt"

	"github.com/gogo/protobuf/types"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
)

type AggregatedQueueServer struct {
	permissions              authorization.PermissionChecker
	schedulingConfig         configuration.SchedulingConfig
	jobRepository            repository.JobRepository
	jobQueue                 scheduling.JobQueue
	queueRepository          repository.QueueRepository
	usageRepository          repository.UsageRepository
	eventStore               repository.EventStore
	schedulingInfoRepository repository.SchedulingInfoRepository
}

func NewAggregatedQueueServer(
	permissions authorization.PermissionChecker,
	schedulingConfig configuration.SchedulingConfig,
	jobRepository repository.JobRepository,
	jobQueue scheduling.JobQueue,
	queueRepository repository.QueueRepository,
	usageRepository repository.UsageRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
) *AggregatedQueueServer {
	return &AggregatedQueueServer{
		permissions:              permissions,
		schedulingConfig:         schedulingConfig,
		jobQueue:                 jobQueue,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		usageRepository:          usageRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository}
}

func (q AggregatedQueueServer) LeaseJobs(ctx context.Context, request *api.LeaseRequest) (*api.JobLease, error) {
	if err := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[LeaseJobs] error: %s", err)
	}

	var res common.ComputeResources = request.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return &api.JobLease{}, nil
	}

	queues, err := q.queueRepository.GetAllQueues()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error getting queues: %s", err)
	}

	activeQueues, err := q.jobRepository.FilterActiveQueues(queues)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error filtering active queues: %s", err)
	}

	usageReports, err := q.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error getting cluster usage: %s", err)
	}

	err = q.usageRepository.UpdateClusterLeased(&request.ClusterLeasedReport)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error updating cluster lease report: %s", err)
	}

	nodeResources := scheduling.AggregateNodeTypeAllocations(request.Nodes)
	clusterSchedulingInfo := scheduling.CreateClusterSchedulingInfoReport(request, nodeResources)
	err = q.schedulingInfoRepository.UpdateClusterSchedulingInfo(clusterSchedulingInfo)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error updating cluster scheduling info: %s", err)
	}

	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	activePoolClusterReports := scheduling.FilterPoolClusters(request.Pool, activeClusterReports)
	activePoolCLusterIds := scheduling.GetClusterReportIds(activePoolClusterReports)
	clusterPriorities, err := q.usageRepository.GetClusterPriorities(activePoolCLusterIds)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error getting cluster priorities: %s", err)
	}

	clusterLeasedJobReports, e := q.usageRepository.GetClusterLeasedReports()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error getting cluster lease reports: %s", err)
	}
	poolLeasedJobReports := scheduling.FilterClusterLeasedReports(activePoolCLusterIds, clusterLeasedJobReports)
	jobs, err := scheduling.LeaseJobs(
		ctx,
		&q.schedulingConfig,
		q.jobQueue,
		func(jobs []*api.Job) { reportJobsLeased(q.eventStore, jobs, request.ClusterId) },
		request,
		nodeResources,
		activePoolClusterReports,
		poolLeasedJobReports,
		clusterPriorities,
		activeQueues)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error leasing jobs: %s", err)
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
	if err := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[RenewLease] error: %s", err)
	}
	renewed, e := q.jobRepository.RenewLease(request.ClusterId, request.Ids)
	return &api.IdList{renewed}, e
}

func (q *AggregatedQueueServer) ReturnLease(ctx context.Context, request *api.ReturnLeaseRequest) (*types.Empty, error) {
	if err := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[ReturnLease] error: %s", err)
	}

	// Check how many times the same job has been retried already
	retries, err := q.jobRepository.GetNumberOfRetryAttempts(request.JobId)
	if err != nil {
		return nil, err
	}

	maxRetries := int(q.schedulingConfig.MaxRetries)
	if retries >= maxRetries {
		failureReason := fmt.Sprintf("Exceeded maximum number of retries: %d", maxRetries)
		err = q.reportFailure(request.JobId, request.ClusterId, failureReason)
		if err != nil {
			return nil, err
		}

		_, err := q.ReportDone(ctx, &api.IdList{Ids: []string{request.JobId}})
		if err != nil {
			return nil, err
		}

		return &types.Empty{}, nil
	}

	if request.AvoidNodeLabels != nil && len(request.AvoidNodeLabels.Entries) > 0 {
		err = q.addAvoidNodeAffinity(request.JobId, request.AvoidNodeLabels, authorization.GetPrincipal(ctx).GetName())
		if err != nil {
			log.Warnf("Failed to set avoid node affinity for job %s: %v", request.JobId, err)
		}
	}

	_, err = q.jobRepository.ReturnLease(request.ClusterId, request.JobId)
	if err != nil {
		return nil, err
	}

	err = q.jobRepository.AddRetryAttempt(request.JobId)
	if err != nil {
		return nil, err
	}

	return &types.Empty{}, nil
}

func (q *AggregatedQueueServer) addAvoidNodeAffinity(jobId string, labels *api.OrderedStringMap, principalName string) error {
	allClusterSchedulingInfo, e := q.schedulingInfoRepository.GetClusterSchedulingInfo()
	if e != nil {
		return e
	}

	res, err := q.jobRepository.UpdateJobs([]string{jobId}, func(jobs []*api.Job) {
		if len(jobs) < 1 {
			log.Warnf("addAvoidNodeAffinity: Job %s not found", jobId)
			return
		}

		changed := addAvoidNodeAffinity(jobs[0], labels, func(jobsToValidate []*api.Job) error {
			return validateJobsCanBeScheduled(jobsToValidate, allClusterSchedulingInfo)
		})

		if changed {
			err := reportJobsUpdated(q.eventStore, principalName, jobs)
			if err != nil {
				log.Warnf("addAvoidNodeAffinity: Failed to report job updated event for job %s: %v", jobId, err)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("[AggregatedQueueServer.addAvoidNodeAffinity] error updating job with ID %s: %s", jobId, err)
	}

	if len(res) < 1 {
		return errors.New("Job not found")
	}

	return res[0].Error
}

func (q *AggregatedQueueServer) ReportDone(ctx context.Context, idList *api.IdList) (*api.IdList, error) {
	if err := checkPermission(q.permissions, ctx, permissions.ExecuteJobs); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "[ReportDone] error: %s", err)
	}
	jobs, e := q.jobRepository.GetExistingJobsByIds(idList.Ids)
	if e != nil {
		return nil, status.Errorf(codes.Internal, e.Error())
	}
	deletionResult, err := q.jobRepository.DeleteJobs(jobs)
	if err != nil {
		return nil, fmt.Errorf("[AggregatedQueueServer.ReportDone] error deleting jobs: %s", err)
	}

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

func (q *AggregatedQueueServer) reportFailure(jobId string, clusterId string, reason string) error {
	job, err := q.getJobById(jobId)
	if err != nil {
		return err
	}

	err = reportFailed(q.eventStore, clusterId, []*jobFailure{{job: job, reason: reason}})
	if err != nil {
		return err
	}

	return nil
}

func (q *AggregatedQueueServer) getJobById(jobId string) (*api.Job, error) {
	jobs, err := q.jobRepository.GetExistingJobsByIds([]string{jobId})
	if err != nil {
		return nil, err
	}
	if len(jobs) < 1 {
		return nil, fmt.Errorf("job with jobId %q not found", jobId)
	}
	return jobs[0], err
}
