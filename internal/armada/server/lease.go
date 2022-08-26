package server

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/gogo/protobuf/types"
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client/queue"
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
		schedulingInfoRepository: schedulingInfoRepository,
	}
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

	activeQueues, e := q.jobRepository.FilterActiveQueues(queue.QueuesToAPI(queues))
	if e != nil {
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

	clusterLeasedJobReports, err := q.usageRepository.GetClusterLeasedReports()
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
	err = q.usageRepository.UpdateClusterLeased(clusterLeasedReport)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "[LeaseJobs] error creating lease report: %s", err)
	}

	jobLease := &api.JobLease{
		Job: jobs,
	}
	return jobLease, nil
}

// StreamingLeaseJobs is called by the executor to request jobs for it to run.
// It streams jobs to the executor as quickly as it can and then waits to receive ids back.
// Only jobs for which an id was sent back are marked as leased.
//
// This function should be used instead of the LeaseJobs function in most cases.
func (q *AggregatedQueueServer) StreamingLeaseJobs(stream api.AggregatedQueue_StreamingLeaseJobsServer) error {
	if err := checkPermission(q.permissions, stream.Context(), permissions.ExecuteJobs); err != nil {
		return err
	}

	// Receive once to get info necessary to get jobs to lease.
	req, err := stream.Recv()
	if err != nil {
		return errors.WithStack(err)
	}

	// Return no jobs if we don't have enough work.
	var res common.ComputeResources = req.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return nil
	}

	queues, err := q.queueRepository.GetAllQueues()
	if err != nil {
		return err
	}

	activeQueues, err := q.jobRepository.FilterActiveQueues(queue.QueuesToAPI(queues))
	if err != nil {
		return err
	}

	usageReports, err := q.usageRepository.GetClusterUsageReports()
	if err != nil {
		return err
	}

	if err = q.usageRepository.UpdateClusterLeased(&req.ClusterLeasedReport); err != nil {
		return err
	}

	// One of the below functions expects a regular lease request,
	// so we need to convert.
	leaseRequest := &api.LeaseRequest{
		ClusterId:           req.ClusterId,
		Pool:                req.Pool,
		Resources:           req.Resources,
		ClusterLeasedReport: req.ClusterLeasedReport,
		MinimumJobSize:      req.MinimumJobSize,
		Nodes:               req.Nodes,
	}
	nodeResources := scheduling.AggregateNodeTypeAllocations(req.Nodes)
	clusterSchedulingInfo := scheduling.CreateClusterSchedulingInfoReport(leaseRequest, nodeResources)
	err = q.schedulingInfoRepository.UpdateClusterSchedulingInfo(clusterSchedulingInfo)
	if err != nil {
		return err
	}

	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	activePoolClusterReports := scheduling.FilterPoolClusters(req.Pool, activeClusterReports)
	activePoolCLusterIds := scheduling.GetClusterReportIds(activePoolClusterReports)
	clusterPriorities, err := q.usageRepository.GetClusterPriorities(activePoolCLusterIds)
	if err != nil {
		return err
	}

	clusterLeasedJobReports, err := q.usageRepository.GetClusterLeasedReports()
	if err != nil {
		return err
	}
	poolLeasedJobReports := scheduling.FilterClusterLeasedReports(activePoolCLusterIds, clusterLeasedJobReports)
	jobs, err := scheduling.LeaseJobs(
		stream.Context(),
		&q.schedulingConfig,
		q.jobQueue,
		// For the unary job lease call, we pass in a function that creates job leased events.
		// Here, we create such events at the end of the function only for jobs the client sent back acks for.
		func(jobs []*api.Job) {},
		leaseRequest,
		nodeResources,
		activePoolClusterReports,
		poolLeasedJobReports,
		clusterPriorities,
		activeQueues)
	if err != nil {
		return err
	}

	// The server streams jobs to the executor.
	// The executor streams back an ack for each received job.
	// With each job sent to the executor, the server includes the number of received acks.
	//
	// When the connection breaks, the server expires all leases for which it hasn't received an ack
	// and the executor expires all leases for which it hasn't received confirmation that the server received the ack.
	//
	// We track the total number of jobs and the number of jobs for which acks have been received.
	// Because gRPC streams guarantee ordering, we only need to track the number of acks.
	// The client is responsible for acking jobs in the order they are received.
	numJobs := uint32(len(jobs))
	var numAcked uint32

	// Stream the jobs to the executor.
	g, _ := errgroup.WithContext(stream.Context())
	g.Go(func() error {
		for _, job := range jobs {
			err := stream.Send(&api.StreamingJobLease{
				Job:      job,
				NumJobs:  numJobs,
				NumAcked: atomic.LoadUint32(&numAcked),
			})
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
		}
		return nil
	})

	// Listen for job ids being streamed back as they're received.
	g.Go(func() error {
		numJobs := numJobs // Assign a local variable to guarantee there are no race conditions.
		for atomic.LoadUint32(&numAcked) < numJobs {
			ack, err := stream.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			atomic.AddUint32(&numAcked, uint32(len(ack.ReceivedJobIds)))
		}
		return nil
	})

	// Wait for all jobs to have been sent and all acks to have been received.
	err = g.Wait()
	if err != nil {
		log.WithError(err).Error("error sending/receiving job leases to/from executor")
	}

	// Send one more message with the total number of acks.
	err = stream.Send(&api.StreamingJobLease{
		Job:      nil, // Omitted
		NumJobs:  numJobs,
		NumAcked: numAcked,
	})
	if err != nil {
		log.WithError(err).Error("error sending the number of acks")
	}

	// Create job leased events and write a leased report into Redis for all acked jobs.
	ackedJobs := jobs[:numAcked]
	reportJobsLeased(q.eventStore, ackedJobs, req.ClusterId)

	var result *multierror.Error
	clusterLeasedReport := scheduling.CreateClusterLeasedReport(req.ClusterLeasedReport.ClusterId, &req.ClusterLeasedReport, ackedJobs)
	err = q.usageRepository.UpdateClusterLeased(clusterLeasedReport)
	result = multierror.Append(result, err)

	// scheduling.LeaseJobs (called above) automatically marks all returned jobs as leased.
	// Return the leases of any non-acked jobs so that they can be re-leased.
	for i := numAcked; i < numJobs; i++ {
		_, err = q.jobRepository.ReturnLease(req.ClusterId, jobs[i].Id)
		result = multierror.Append(result, err)
	}

	return result.ErrorOrNil()
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

	err = q.reportLeaseReturned(request)
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
	allClusterSchedulingInfo, err := q.schedulingInfoRepository.GetClusterSchedulingInfo()
	if err != nil {
		return fmt.Errorf("[AggregatedQueueServer.addAvoidNodeAffinity] error getting scheduling information: %w", err)
	}

	res, err := q.jobRepository.UpdateJobs([]string{jobId}, func(jobs []*api.Job) {
		if len(jobs) < 1 {
			log.Warnf("[AggregatedQueueServer.addAvoidNodeAffinity] job %s not found", jobId)
			return
		}

		changed := addAvoidNodeAffinity(jobs[0], labels, func(jobsToValidate []*api.Job) error {
			if ok, err := validateJobsCanBeScheduled(jobsToValidate, allClusterSchedulingInfo); !ok {
				if err != nil {
					return errors.WithMessage(err, "can't schedule at least 1 job")
				} else {
					return errors.Errorf("can't schedule at least 1 job")
				}
			}
			return nil
		})

		if changed {
			err := reportJobsUpdated(q.eventStore, principalName, jobs)
			if err != nil {
				log.Warnf("[AggregatedQueueServer.addAvoidNodeAffinity] error reporting job updated event for job %s: %s", jobId, err)
			}
		}
	})
	if err != nil {
		return fmt.Errorf("[AggregatedQueueServer.addAvoidNodeAffinity] error updating job with ID %s: %s", jobId, err)
	}

	if len(res) < 1 {
		errJobNotFound := &repository.ErrJobNotFound{JobId: jobId}
		return fmt.Errorf("[AggregatedQueueServer.addAvoidNodeAffinity] error: %w", errJobNotFound)
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

func (q *AggregatedQueueServer) reportLeaseReturned(leaseReturnRequest *api.ReturnLeaseRequest) error {
	job, err := q.getJobById(leaseReturnRequest.JobId)
	if err != nil {
		return err
	}

	err = reportJobLeaseReturned(q.eventStore, job, leaseReturnRequest)
	if err != nil {
		return err
	}

	return nil
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
