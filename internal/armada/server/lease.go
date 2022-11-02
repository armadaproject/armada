package server

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/hashicorp/go-multierror"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/clock"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/armada/permissions"
	"github.com/G-Research/armada/internal/armada/repository"
	"github.com/G-Research/armada/internal/armada/scheduling"
	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/common/logging"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
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
	decompressorPool         *pool.ObjectPool
	clock                    clock.Clock
	// For storing reports of scheduling attempts.
	SchedulingReportsRepository *scheduler.SchedulingReportsRepository
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
	poolConfig := pool.ObjectPoolConfig{
		MaxTotal:                 100,
		MaxIdle:                  50,
		MinIdle:                  10,
		BlockWhenExhausted:       true,
		MinEvictableIdleTime:     30 * time.Minute,
		SoftMinEvictableIdleTime: math.MaxInt64,
		TimeBetweenEvictionRuns:  0,
		NumTestsPerEvictionRun:   10,
	}

	decompressorPool := pool.NewObjectPool(context.Background(), pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			return compress.NewZlibDecompressor()
		}), &poolConfig)
	return &AggregatedQueueServer{
		permissions:              permissions,
		schedulingConfig:         schedulingConfig,
		jobQueue:                 jobQueue,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		usageRepository:          usageRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository,
		decompressorPool:         decompressorPool,
		clock:                    clock.RealClock{},
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

	err = q.decompressJobOwnershipGroups(jobs)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "error leasing jobs: %s", err)
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

	// Old scheduler resource accounting logic. Should be called on all requests.
	err = q.usageRepository.UpdateClusterLeased(&req.ClusterLeasedReport)
	if err != nil {
		return err
	}
	leaseRequest := leaseRequestFromStreamingLeaseRequest(req)
	nodeResources := scheduling.AggregateNodeTypeAllocations(req.Nodes)
	clusterSchedulingInfo := scheduling.CreateClusterSchedulingInfoReport(leaseRequest, nodeResources)
	err = q.schedulingInfoRepository.UpdateClusterSchedulingInfo(clusterSchedulingInfo)
	if err != nil {
		return err
	}

	// New scheduler resource accounting logic.
	usageByQueue := make(map[string]*schedulerobjects.QueueClusterResourceUsage)
	for _, r := range req.GetClusterLeasedReport().Queues {
		resourcesByPriority := make(map[int32]schedulerobjects.ResourceList)
		for p, rs := range r.ResourcesLeasedByPriority {
			resourcesByPriority[p] = schedulerobjects.ResourceList{
				Resources: make(map[string]resource.Quantity),
			}
			for t, q := range rs.Resources {
				resourcesByPriority[p].Resources[t] = q.DeepCopy()
			}
		}
		report := &schedulerobjects.QueueClusterResourceUsage{
			Created:             q.clock.Now(),
			Queue:               r.Name,
			ExecutorId:          req.GetClusterLeasedReport().ClusterId,
			ResourcesByPriority: resourcesByPriority,
		}
		usageByQueue[r.Name] = report
	}
	clusterUsageReport := q.createClusterUsageReport(usageByQueue, req.Pool)
	err = q.usageRepository.UpdateClusterQueueResourceUsage(req.ClusterId, clusterUsageReport)
	if err != nil {
		return err
	}

	// Return no jobs if we don't have enough work.
	var res common.ComputeResources = req.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return nil
	}

	// Get jobs to be leased.
	var jobs []*api.Job
	if rand.Float64() < q.schedulingConfig.ProbabilityOfUsingNewScheduler {
		// New scheduler.
		jobs, err = q.getJobs(stream.Context(), req)
		if err != nil {
			return err
		}
	} else {
		// Old scheduler.
		jobs, err = q.legacyGetJobs(stream.Context(), leaseRequest)
		if err != nil {
			return err
		}
	}

	err = q.decompressJobOwnershipGroups(jobs)
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

func leaseRequestFromStreamingLeaseRequest(req *api.StreamingLeaseRequest) *api.LeaseRequest {
	return &api.LeaseRequest{
		ClusterId:           req.ClusterId,
		Pool:                req.Pool,
		Resources:           req.Resources,
		ClusterLeasedReport: req.ClusterLeasedReport,
		MinimumJobSize:      req.MinimumJobSize,
		Nodes:               req.Nodes,
	}
}

func (q *AggregatedQueueServer) legacyGetJobs(ctx context.Context, req *api.LeaseRequest) ([]*api.Job, error) {
	log := ctxlogrus.Extract(ctx)
	log.Info("using old scheduler for lease call")

	usageReports, err := q.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, err
	}
	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	activePoolClusterReports := scheduling.FilterPoolClusters(req.Pool, activeClusterReports)
	activePoolCLusterIds := scheduling.GetClusterReportIds(activePoolClusterReports)
	clusterPriorities, err := q.usageRepository.GetClusterPriorities(activePoolCLusterIds)
	if err != nil {
		return nil, err
	}

	queues, err := q.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}

	activeQueues, err := q.jobRepository.FilterActiveQueues(queue.QueuesToAPI(queues))
	if err != nil {
		return nil, err
	}

	clusterLeasedJobReports, err := q.usageRepository.GetClusterLeasedReports()
	if err != nil {
		return nil, err
	}

	nodeResources := scheduling.AggregateNodeTypeAllocations(req.Nodes)
	poolLeasedJobReports := scheduling.FilterClusterLeasedReports(activePoolCLusterIds, clusterLeasedJobReports)
	jobs, err := scheduling.LeaseJobs(
		ctx,
		&q.schedulingConfig,
		q.jobQueue,
		// For the unary job lease call, we pass in a function that creates job leased events.
		// Here, we create such events at the end of the function only for jobs the client sent back acks for.
		func(jobs []*api.Job) {},
		req,
		nodeResources,
		activePoolClusterReports,
		poolLeasedJobReports,
		clusterPriorities,
		activeQueues,
	)
	if err != nil {
		return nil, err
	}

	return jobs, nil
}

func (q *AggregatedQueueServer) getJobs(ctx context.Context, req *api.StreamingLeaseRequest) ([]*api.Job, error) {
	log := ctxlogrus.Extract(ctx)
	log.Info("using new scheduler for lease call")

	// Get the total capacity available across all clusters.
	usageReports, err := q.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, err
	}
	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	totalCapacity := make(common.ComputeResources)
	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(util.GetClusterAvailableCapacity(clusterReport))
	}
	totalCapacityRl := schedulerobjects.ResourceList{
		Resources: totalCapacity,
	}

	// load the usage from all other executors
	reportsByExecutor, err := q.usageRepository.GetClusterQueueResourceUsage()
	if err != nil {
		return nil, err
	}

	// Create an aggregated usage by queue over all clusters
	aggregatedUsageByQueue := q.aggregateUsage(reportsByExecutor, req.Pool)

	// Collect all allowed priorities.
	priorities := make([]int32, 0)
	if len(q.schedulingConfig.Preemption.PriorityClasses) > 0 {
		for _, p := range q.schedulingConfig.Preemption.PriorityClasses {
			priorities = append(priorities, p.Priority)
		}
	} else {
		priorities = append(priorities, 0)
	}

	// Nodes to be considered by the scheduler.
	nodes := make([]*schedulerobjects.Node, len(req.Nodes))
	for i, nodeInfo := range req.Nodes {
		nodes[i] = schedulerobjects.NewNodeFromNodeInfo(
			&nodeInfo,
			req.ClusterId,
			priorities,
			q.schedulingConfig.IndexedTaints,
			q.schedulingConfig.IndexedNodeLabels,
		)
	}

	// Map queue names to priority factor for all active queues, i.e.,
	// all queues for which the jobs queue has not been deleted automatically by Redis.
	queues, err := q.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}
	priorityFactorByQueue := make(map[string]float64)
	apiQueues := make([]*api.Queue, len(queues))
	for i, queue := range queues {
		priorityFactorByQueue[queue.Name] = float64(queue.PriorityFactor)
		apiQueues[i] = &api.Queue{Name: queue.Name}
	}
	activeQueues, err := q.jobRepository.FilterActiveQueues(apiQueues)
	if err != nil {
		return nil, err
	}
	priorityFactorByActiveQueue := make(map[string]float64)
	for _, queue := range activeQueues {
		priorityFactorByActiveQueue[queue.Name] = priorityFactorByQueue[queue.Name]
	}

	sched, err := scheduler.NewLegacyScheduler(
		q.schedulingConfig,
		req.ClusterId,
		totalCapacityRl,
		nodes,
		q.jobRepository,
		priorityFactorByActiveQueue,
	)
	if err != nil {
		return nil, err
	}
	sched.MinimumJobSize = req.MinimumJobSize
	sched.Pool = req.Pool

	// Log initial scheduler state.
	log.Info("LegacyScheduler:\n" + sched.String())

	// Give Schedule() a 3 second shorter deadline than ctx,
	// to give it a chance to finish up before ctx is cancelled.
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-3*time.Second))
		defer cancel()
	}

	// Run the scheduler.
	jobs, err := sched.Schedule(ctx, aggregatedUsageByQueue)

	// Log and store scheduling reports.
	if sched.SchedulingRoundReport != nil {
		log.Infof("Scheduling report:\n%s", sched.SchedulingRoundReport)
		if q.SchedulingReportsRepository != nil {
			sched.SchedulingRoundReport.ClearJobSpecs()
			for queue, jobReports := range sched.SchedulingRoundReport.SuccessfulJobSchedulingReportsByQueue {
				for _, jobReport := range jobReports {
					q.SchedulingReportsRepository.Add(queue, jobReport)
				}
			}
			for queue, jobReports := range sched.SchedulingRoundReport.UnsuccessfulJobSchedulingReportsByQueue {
				for _, jobReport := range jobReports {
					q.SchedulingReportsRepository.Add(queue, jobReport)
				}
			}
		}
	}

	// Update the usage report in-place to account for any leased jobs and write it back into Redis.
	// This ensures resources of leased jobs are accounted for without needing to wait for feedback from the executor.
	if sched.SchedulingRoundReport != nil {
		executorReport, ok := reportsByExecutor[req.ClusterId]
		if !ok || executorReport.ResourcesByQueue == nil {
			executorReport = &schedulerobjects.ClusterResourceUsageReport{
				Pool:             req.Pool,
				Created:          q.clock.Now(),
				ResourcesByQueue: make(map[string]*schedulerobjects.QueueClusterResourceUsage),
			}
			reportsByExecutor[req.ClusterId] = executorReport
		}
		for queue, resourcesByPriority := range sched.SchedulingRoundReport.ScheduledResourcesByQueueAndPriority {
			if queueClusterUsage, ok := executorReport.ResourcesByQueue[queue]; ok && queueClusterUsage != nil && queueClusterUsage.ResourcesByPriority != nil {
				schedulerobjects.QuantityByPriorityAndResourceType(queueClusterUsage.ResourcesByPriority).Add(resourcesByPriority)
			} else {
				queueClusterUsage = &schedulerobjects.QueueClusterResourceUsage{
					Created:             q.clock.Now(),
					Queue:               queue,
					ExecutorId:          req.ClusterId,
					ResourcesByPriority: resourcesByPriority,
				}
				executorReport.ResourcesByQueue[queue] = queueClusterUsage
			}
		}
		if err := q.usageRepository.UpdateClusterQueueResourceUsage(req.ClusterId, executorReport); err != nil {
			logging.WithStacktrace(log, err).Errorf("failed to update cluster usage")
		}
	}

	// If we successfully leased jobs, those have to be sent to the executor,
	// since the leases will have been written into Redis.
	if len(jobs) > 0 {
		if err != nil {
			logging.WithStacktrace(log, err).Error("failed to schedule jobs")
		}
		return jobs, nil
	} else if err != nil {
		return nil, err
	}

	return jobs, nil
}

// aggregateUsage Creates a map of usage first by cluster and then by queue
// Note that the desired cluster is excluded as this will be filled in later as are clusters that are not in the
// same pool as the desired cluster
func (q *AggregatedQueueServer) aggregateUsage(reportsByCluster map[string]*schedulerobjects.ClusterResourceUsageReport, pool string) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	const activeClusterExpiry = 10 * time.Minute
	now := q.clock.Now()

	// Aggregate resource usage across clusters.
	aggregatedUsageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, clusterReport := range reportsByCluster {
		if clusterReport.Pool == pool && clusterReport.Created.Add(activeClusterExpiry).After(now) {
			for queue, report := range clusterReport.ResourcesByQueue {
				quantityByPriorityAndResourceType, ok := aggregatedUsageByQueue[queue]
				if !ok {
					quantityByPriorityAndResourceType = make(schedulerobjects.QuantityByPriorityAndResourceType)
					aggregatedUsageByQueue[queue] = quantityByPriorityAndResourceType
				}
				quantityByPriorityAndResourceType.Add(report.ResourcesByPriority)
			}
		}
	}
	return aggregatedUsageByQueue
}

// createClusterUsageReport creates a schedulerobjects.ClusterResourceUsageReport suitable for storing in redis
func (q *AggregatedQueueServer) createClusterUsageReport(resourceByQueue map[string]*schedulerobjects.QueueClusterResourceUsage, pool string) *schedulerobjects.ClusterResourceUsageReport {
	return &schedulerobjects.ClusterResourceUsageReport{
		Pool:             pool,
		Created:          q.clock.Now(),
		ResourcesByQueue: resourceByQueue,
	}
}

func (q *AggregatedQueueServer) decompressJobOwnershipGroups(jobs []*api.Job) error {
	for _, j := range jobs {
		// No need to decompress, if compressed groups not set
		if len(j.CompressedQueueOwnershipUserGroups) == 0 {
			continue
		}
		groups, err := q.decompressOwnershipGroups(j.CompressedQueueOwnershipUserGroups)
		if err != nil {
			return fmt.Errorf("failed to decompress ownership groups for job %s because %s", j.Id, err)
		}
		j.QueueOwnershipUserGroups = groups
		j.CompressedQueueOwnershipUserGroups = nil
	}

	return nil
}

func (q *AggregatedQueueServer) decompressOwnershipGroups(compressedOwnershipGroups []byte) ([]string, error) {
	decompressor, err := q.decompressorPool.BorrowObject(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to borrow decompressior because %s", err)
	}

	defer func(decompressorPool *pool.ObjectPool, ctx context.Context, object interface{}) {
		err := decompressorPool.ReturnObject(ctx, object)
		if err != nil {
			log.WithError(err).Errorf("Error returning decompressorPool to pool")
		}
	}(q.decompressorPool, context.Background(), decompressor)

	return compress.DecompressStringArray(compressedOwnershipGroups, decompressor.(compress.Decompressor))
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

func (q *AggregatedQueueServer) addAvoidNodeAffinity(
	jobId string,
	labels *api.OrderedStringMap,
	principalName string,
) error {
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
		return nil, errors.Errorf("job with jobId %q not found", jobId)
	}
	return jobs[0], err
}
