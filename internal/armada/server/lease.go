package server

import (
	"context"
	"fmt"
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/gogo/protobuf/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/hashicorp/go-multierror"
	pool "github.com/jolestar/go-commons-pool"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/armada/permissions"
	"github.com/armadaproject/armada/internal/armada/repository"
	"github.com/armadaproject/armada/internal/armada/scheduling"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/compress"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	armadaresource "github.com/armadaproject/armada/internal/common/resource"
	"github.com/armadaproject/armada/internal/common/schedulers"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

type AggregatedQueueServer struct {
	permissions              authorization.PermissionChecker
	schedulingConfig         configuration.SchedulingConfig
	jobRepository            repository.JobRepository
	queueRepository          repository.QueueRepository
	usageRepository          repository.UsageRepository
	eventStore               repository.EventStore
	schedulingInfoRepository repository.SchedulingInfoRepository
	decompressorPool         *pool.ObjectPool
	clock                    clock.Clock
	// For storing reports of scheduling attempts.
	SchedulingReportsRepository *scheduler.SchedulingReportsRepository
	// Stores the most recent NodeDb for each executor.
	// Used to check if a job could ever be scheduled at job submit time.
	SubmitChecker *scheduler.SubmitChecker
	// Necessary to generate preempted messages.
	pulsarProducer       pulsar.Producer
	maxPulsarMessageSize uint
	executorRepository   database.ExecutorRepository
}

func NewAggregatedQueueServer(
	permissions authorization.PermissionChecker,
	schedulingConfig configuration.SchedulingConfig,
	jobRepository repository.JobRepository,
	queueRepository repository.QueueRepository,
	usageRepository repository.UsageRepository,
	eventStore repository.EventStore,
	schedulingInfoRepository repository.SchedulingInfoRepository,
	pulsarProducer pulsar.Producer,
	maxPulsarMessageSize uint,
	executorRepository database.ExecutorRepository,
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
			return compress.NewZlibDecompressor(), nil
		}), &poolConfig)
	return &AggregatedQueueServer{
		permissions:              permissions,
		schedulingConfig:         schedulingConfig,
		jobRepository:            jobRepository,
		queueRepository:          queueRepository,
		usageRepository:          usageRepository,
		eventStore:               eventStore,
		schedulingInfoRepository: schedulingInfoRepository,
		decompressorPool:         decompressorPool,
		executorRepository:       executorRepository,
		clock:                    clock.RealClock{},
		pulsarProducer:           pulsarProducer,
		maxPulsarMessageSize:     maxPulsarMessageSize,
	}
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

	// Old scheduler resource accounting logic.
	err = q.usageRepository.UpdateClusterLeased(&req.ClusterLeasedReport)
	if err != nil {
		return err
	}
	nodeResources := scheduling.AggregateNodeTypeAllocations(req.Nodes)
	clusterSchedulingInfo := scheduling.CreateClusterSchedulingInfoReport(req, nodeResources)
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

	clusterUsageReport := &schedulerobjects.ClusterResourceUsageReport{
		Pool:             req.Pool,
		Created:          q.clock.Now(),
		ResourcesByQueue: usageByQueue,
	}
	err = q.usageRepository.UpdateClusterQueueResourceUsage(req.ClusterId, clusterUsageReport)
	if err != nil {
		return err
	}

	// Return no jobs if we don't have enough work.
	var res armadaresource.ComputeResources = req.Resources
	if res.AsFloat().IsLessThan(q.schedulingConfig.MinimumResourceToSchedule) {
		return nil
	}

	// Get jobs to be leased.
	jobs, err := q.getJobs(stream.Context(), req)
	if err != nil {
		return err
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

type SchedulerJobRepositoryAdapter struct {
	r repository.JobRepository
}

func (repo *SchedulerJobRepositoryAdapter) GetQueueJobIds(queue string) ([]string, error) {
	return repo.r.GetQueueJobIds(queue)
}

func (repo *SchedulerJobRepositoryAdapter) GetExistingJobsByIds(ids []string) ([]scheduler.LegacySchedulerJob, error) {
	jobs, err := repo.r.GetExistingJobsByIds(ids)
	if err != nil {
		return nil, err
	}
	rv := make([]scheduler.LegacySchedulerJob, len(jobs))
	for i, job := range jobs {
		rv[i] = job
	}
	return rv, nil
}

func (q *AggregatedQueueServer) getJobs(ctx context.Context, req *api.StreamingLeaseRequest) ([]*api.Job, error) {
	log := ctxlogrus.Extract(ctx)
	log = log.WithFields(logrus.Fields{
		"function": "getJobs",
		"cluster":  req.ClusterId,
	})
	ctx = ctxlogrus.ToContext(ctx, log)

	// Get the total capacity available across all clusters.
	usageReports, err := q.usageRepository.GetClusterUsageReports()
	if err != nil {
		return nil, err
	}
	activeClusterReports := scheduling.FilterActiveClusters(usageReports)
	totalCapacity := make(armadaresource.ComputeResources)
	for _, clusterReport := range activeClusterReports {
		totalCapacity.Add(util.GetClusterAvailableCapacity(clusterReport))
	}

	// Load and aggregate usage across executor clusters.
	reportsByExecutor, err := q.usageRepository.GetClusterQueueResourceUsage()
	if err != nil {
		return nil, err
	}
	aggregatedUsageByQueue := q.aggregateUsage(reportsByExecutor, req.Pool)
	log.Info("aggregated resource usage prior to scheduling: %v", aggregatedUsageByQueue)

	// Collect all allowed priorities.
	allowedPriorities := q.schedulingConfig.Preemption.AllowedPriorities()
	if len(allowedPriorities) == 0 {
		return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "PriorityClasses",
			Value:   q.schedulingConfig.Preemption.PriorityClasses,
			Message: "there must be at least one supported priority",
		})
	}

	// Nodes to be considered by the scheduler.
	lastSeen := q.clock.Now()
	nodes := make([]*schedulerobjects.Node, 0, len(req.Nodes))
	for _, nodeInfo := range req.Nodes {
		node, err := api.NewNodeFromNodeInfo(
			&nodeInfo,
			req.ClusterId,
			allowedPriorities,
			lastSeen,
		)
		if err != nil {
			logging.WithStacktrace(log, err).Warnf(
				"skipping node %s from executor %s", nodeInfo.GetName(), req.GetClusterId(),
			)
			continue
		}

		jobIds := make([]string, 0, len(nodeInfo.RunIdsByState))
		for jobId, jobState := range nodeInfo.RunIdsByState {
			if !jobState.IsTerminal() {
				jobIds = append(jobIds, jobId)
			}
		}

		jobs, err := q.jobRepository.GetExistingJobsByIds(jobIds)
		if err != nil {
			return nil, err
		}
		receivedJobIds := make(map[string]bool)
		for _, job := range jobs {
			receivedJobIds[job.Id] = true
		}
		missingJobIds := make([]string, 0)
		for _, jobId := range jobIds {
			if !receivedJobIds[jobId] {
				missingJobIds = append(missingJobIds, jobId)
			}
		}
		if len(missingJobIds) > 0 {
			log.Warnf(
				"skipping node %s from executor %s: failed to load from Redis jobs %v",
				nodeInfo.GetName(), req.GetClusterId(), missingJobIds,
			)
			continue
		}

		// Bind pods to nodes, thus ensuring resources are marked allocated on the node.
		skipNode := false
		for _, job := range jobs {
			node, err = scheduler.BindPodToNode(
				scheduler.PodRequirementFromLegacySchedulerJob(
					job,
					q.schedulingConfig.Preemption.PriorityClasses,
				),
				node,
			)
			if err != nil {
				logging.WithStacktrace(log, err).Warnf(
					"skipping node %s from executor %s: failed to bind job %s to node",
					nodeInfo.GetName(), req.GetClusterId(), job.Id,
				)
				skipNode = true
				break
			}
		}
		if skipNode {
			continue
		}
		nodes = append(nodes, node)
	}
	indexedResources := q.schedulingConfig.IndexedResources
	if len(indexedResources) == 0 {
		indexedResources = []string{"cpu", "memory"}
	}
	nodeDb, err := scheduler.NewNodeDb(
		q.schedulingConfig.Preemption.PriorityClasses,
		indexedResources,
		q.schedulingConfig.IndexedTaints,
		q.schedulingConfig.IndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := nodeDb.UpsertMany(nodes); err != nil {
		return nil, err
	}

	// Store the executor details in redis so that they can be used by
	// submit check and the new scheduler.
	err = q.executorRepository.StoreExecutor(ctx, &schedulerobjects.Executor{
		Id:             req.ClusterId,
		Pool:           req.Pool,
		Nodes:          nodes,
		MinimumJobSize: schedulerobjects.ResourceList{Resources: req.MinimumJobSize},
		LastUpdateTime: time.Now(),
	})
	if err != nil {
		// This is not fatal because we can still schedule if it doesn't happen.
		log.WithError(err).Warnf("could not store executor details for cluster %s", req.ClusterId)
	}

	// Map queue names to priority factor for all active queues, i.e.,
	// all queues for which the jobs queue has not been deleted automatically by Redis.
	queues, err := q.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}
	priorityFactorByQueue := make(map[string]float64, len(queues))
	apiQueues := make([]*api.Queue, len(queues))
	for i, queue := range queues {
		priorityFactorByQueue[queue.Name] = float64(queue.PriorityFactor)
		apiQueues[i] = &api.Queue{Name: queue.Name}
	}
	activeQueues, err := q.jobRepository.FilterActiveQueues(apiQueues)
	if err != nil {
		return nil, err
	}
	priorityFactorByActiveQueue := make(map[string]float64, len(activeQueues))
	for _, queue := range activeQueues {
		priorityFactorByActiveQueue[queue.Name] = priorityFactorByQueue[queue.Name]
	}

	// Give Schedule() a 3 second shorter deadline than ctx,
	// to give it a chance to finish up before ctx is cancelled.
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline.Add(-3*time.Second))
		defer cancel()
	}
	constraints := scheduler.SchedulingConstraintsFromSchedulingConfig(
		req.ClusterId,
		req.Pool,
		schedulerobjects.ResourceList{Resources: req.MinimumJobSize},
		q.schedulingConfig,
		schedulerobjects.ResourceList{Resources: totalCapacity},
	)

	var preemptedJobs []scheduler.LegacySchedulerJob
	var scheduledJobs []scheduler.LegacySchedulerJob
	var nodesByJobId map[string]*schedulerobjects.Node
	if q.schedulingConfig.Preemption.PreemptToFairShare {
		preemptedJobs, scheduledJobs, nodesByJobId, _, err = scheduler.Reschedule(
			ctx,
			&SchedulerJobRepositoryAdapter{
				r: q.jobRepository,
			},
			*constraints,
			q.schedulingConfig,
			nodeDb,
			// May need priority factors for inactive queues for rescheduling evicted jobs.
			priorityFactorByQueue,
			aggregatedUsageByQueue,
			q.schedulingConfig.Preemption.NodeEvictionProbability,
			q.schedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
		)
		if err != nil {
			return nil, err
		}
	} else {
		schedulerQueues := make([]*scheduler.Queue, len(activeQueues))
		for i, apiQueue := range activeQueues {
			jobIterator, err := scheduler.NewQueuedJobsIterator(
				ctx,
				apiQueue.Name,
				&SchedulerJobRepositoryAdapter{
					r: q.jobRepository,
				},
			)
			if err != nil {
				return nil, err
			}
			queue, err := scheduler.NewQueue(
				apiQueue.Name,
				priorityFactorByActiveQueue[apiQueue.Name],
				jobIterator,
			)
			if err != nil {
				return nil, err
			}
			schedulerQueues[i] = queue
		}
		sched, err := scheduler.NewLegacyScheduler(
			ctx,
			*constraints,
			q.schedulingConfig,
			nodeDb,
			schedulerQueues,
			aggregatedUsageByQueue,
		)
		if err != nil {
			return nil, err
		}

		// Log initial scheduler state.
		log.Info("LegacyScheduler:\n" + sched.String())

		// Run the scheduler.
		scheduledJobs, err = sched.Schedule()
		if err != nil {
			return nil, err
		}
	}

	// Prepare preempted messages.
	sequences := make([]*armadaevents.EventSequence, len(preemptedJobs))
	for i, job := range preemptedJobs {
		jobId, err := armadaevents.ProtoUuidFromUlidString(job.GetId())
		if err != nil {
			return nil, err
		}
		created := q.clock.Now()
		sequences[i] = &armadaevents.EventSequence{
			Queue:      job.GetQueue(),
			JobSetName: job.GetJobSet(),
			Events: []*armadaevents.EventSequence_Event{
				{
					Created: &created,
					Event: &armadaevents.EventSequence_Event_JobRunPreempted{
						JobRunPreempted: &armadaevents.JobRunPreempted{
							// Until the executor supports runs properly, JobId and RunId are the same.
							PreemptedJobId: jobId,
							PreemptedRunId: jobId,
						},
					},
				},
			},
		}
	}

	scheduledApiJobsById := make(map[string]*api.Job)
	for _, job := range scheduledJobs {
		if apiJob, ok := job.(*api.Job); ok {
			scheduledApiJobsById[job.GetId()] = apiJob
		} else {
			log.Errorf("failed to convert job %s to api job", job.GetId())
		}
	}

	// Optionally set node id selectors on scheduled jobs.
	if q.schedulingConfig.Preemption.SetNodeIdSelector {
		for jobId, apiJob := range scheduledApiJobsById {
			if apiJob == nil {
				continue
			}
			for _, podSpec := range apiJob.GetAllPodSpecs() {
				if podSpec == nil {
					log.Warnf("failed to set node id selector on job %s: missing pod spec", jobId)
					continue
				}
				node := nodesByJobId[jobId]
				if node == nil {
					log.Warnf("failed to set node id selector on job %s: no node assigned to job", jobId)
					continue
				}
				nodeId := node.Labels[q.schedulingConfig.Preemption.NodeIdLabel]
				if nodeId == "" {
					log.Warnf(
						"failed to set node id selector on job %s to target node %s: nodeIdLabel missing from %s",
						jobId, node.Name, node.Labels,
					)
					continue
				}
				if podSpec.NodeSelector == nil {
					podSpec.NodeSelector = make(map[string]string)
				}
				podSpec.NodeSelector[q.schedulingConfig.Preemption.NodeIdLabel] = nodeId
			}
		}
	}

	// Optionally set node names on scheduled jobs.
	if q.schedulingConfig.Preemption.SetNodeName {
		for jobId, apiJob := range scheduledApiJobsById {
			if apiJob == nil {
				continue
			}
			for _, podSpec := range apiJob.GetAllPodSpecs() {
				if podSpec == nil {
					log.Warnf("failed to set node name on job %s: missing pod spec", jobId)
					continue
				}
				node := nodesByJobId[jobId]
				if node == nil {
					log.Warnf("failed to set node name on job %s: no node assigned to job", jobId)
					continue
				}
				podSpec.NodeName = node.Name
			}
		}
	}

	// Create leases by writing into Redis.
	leasedJobIdsByQueue, err := q.jobRepository.TryLeaseJobs(
		req.ClusterId,
		armadaslices.MapAndGroupByFuncs(
			maps.Values(scheduledApiJobsById),
			func(job *api.Job) string { return job.GetQueue() },
			func(job *api.Job) string { return job.GetId() },
		),
	)
	if err != nil {
		return nil, err
	}
	successfullyLeasedApiJobs := make([]*api.Job, 0, len(scheduledJobs))
	for _, jobIds := range leasedJobIdsByQueue {
		for _, jobId := range jobIds {
			if apiJob, ok := scheduledApiJobsById[jobId]; ok {
				successfullyLeasedApiJobs = append(successfullyLeasedApiJobs, apiJob)
			} else {
				log.Errorf("didn't expect job %s to be leased", jobId)
			}
		}
	}

	// Publish preempted messages.
	if q.pulsarProducer != nil {
		err = pulsarutils.CompactAndPublishSequences(ctx, sequences, q.pulsarProducer, q.maxPulsarMessageSize, schedulers.All)
		if err != nil {
			logging.WithStacktrace(log, err).Error("failed to publish preempted messages")
		}
	} else {
		log.Error("no Pulsar producer provided; omitting publishing preempted messages")
	}

	// Delete preempted jobs from Redis. This will cause lease renewal to fail for these jobs.
	// TODO: We can avoid potential inconsistency by deleting in a pulsar subscriber instead.
	preemptedApiJobs := make([]*api.Job, len(preemptedJobs))
	for i, job := range preemptedJobs {
		preemptedApiJobs[i] = job.(*api.Job)
	}
	if _, err := q.jobRepository.DeleteJobs(preemptedApiJobs); err != nil {
		logging.WithStacktrace(log, err).Errorf(
			"failed to delete preempted jobs: %v",
			armadaslices.Map(preemptedApiJobs, func(job *api.Job) string { return job.GetId() }),
		)
	}

	// TODO: Re-enable if we need to be able to report successful scheduling attempts.
	// // Log and store scheduling reports.
	// if q.SchedulingReportsRepository != nil && sched.SchedulingRoundReport != nil {
	// 	log.Infof("Scheduling report:\n%s", sched.SchedulingRoundReport)
	// 	sched.SchedulingRoundReport.ClearJobSpecs()
	// 	q.SchedulingReportsRepository.AddSchedulingRoundReport(sched.SchedulingRoundReport)
	// }

	// Update the usage report for this executor in-place to account for preempted/leased jobs and write it back into Redis.
	// This ensures rpeempted/leased jobs are accounted for without needing to wait for feedback from the executor.
	executorReport, ok := reportsByExecutor[req.ClusterId]
	if !ok || executorReport.ResourcesByQueue == nil {
		executorReport = &schedulerobjects.ClusterResourceUsageReport{
			Pool:             req.Pool,
			Created:          q.clock.Now(),
			ResourcesByQueue: make(map[string]*schedulerobjects.QueueClusterResourceUsage),
		}
		reportsByExecutor[req.ClusterId] = executorReport
	}
	aggregatedUsageByQueueForCluster := q.aggregateUsage(
		// We only want resources for this cluster.
		map[string]*schedulerobjects.ClusterResourceUsageReport{
			req.ClusterId: executorReport,
		},
		req.Pool,
	)
	aggregatedUsageByQueueForCluster = scheduler.UpdateUsage(
		aggregatedUsageByQueueForCluster,
		preemptedJobs,
		q.schedulingConfig.Preemption.PriorityClasses,
		scheduler.Subtract,
	)
	aggregatedUsageByQueueForCluster = scheduler.UpdateUsage(
		aggregatedUsageByQueueForCluster,
		successfullyLeasedApiJobs,
		q.schedulingConfig.Preemption.PriorityClasses,
		scheduler.Add,
	)
	for queue, usage := range aggregatedUsageByQueueForCluster {
		executorReport.ResourcesByQueue[queue] = &schedulerobjects.QueueClusterResourceUsage{
			Created:             q.clock.Now(),
			Queue:               queue,
			ExecutorId:          req.ClusterId,
			ResourcesByPriority: usage.DeepCopy(),
		}
	}
	if err := q.usageRepository.UpdateClusterQueueResourceUsage(req.ClusterId, executorReport); err != nil {
		logging.WithStacktrace(log, err).Errorf("failed to update cluster usage")
	}

	aggregatedUsageByQueue = q.aggregateUsage(reportsByExecutor, req.Pool)
	log.Info("aggregated resource usage after scheduling: %v", aggregatedUsageByQueue)

	return successfullyLeasedApiJobs, nil
}

// aggregateUsage Creates a map of resource usage first by cluster and then by queue.
// Clusters in pools other than pool are excluded.
func (q *AggregatedQueueServer) aggregateUsage(reportsByCluster map[string]*schedulerobjects.ClusterResourceUsageReport, pool string) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	const activeClusterExpiry = 10 * time.Minute
	now := q.clock.Now()
	aggregatedUsageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, clusterReport := range reportsByCluster {
		if clusterReport.Pool != pool {
			// Separate resource accounting per pool.
			continue
		}
		if !clusterReport.Created.Add(activeClusterExpiry).After(now) {
			// Stale report; omit.
			continue
		}
		for queue, report := range clusterReport.ResourcesByQueue {
			quantityByPriorityAndResourceType, ok := aggregatedUsageByQueue[queue]
			if !ok {
				quantityByPriorityAndResourceType = make(schedulerobjects.QuantityByPriorityAndResourceType)
				aggregatedUsageByQueue[queue] = quantityByPriorityAndResourceType
			}
			quantityByPriorityAndResourceType.Add(report.ResourcesByPriority)
		}
	}
	return aggregatedUsageByQueue
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

	if request.JobRunAttempted {
		err = q.jobRepository.AddRetryAttempt(request.JobId)
		if err != nil {
			return nil, err
		}
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
