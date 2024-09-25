package scheduling

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/benbjohnson/immutable"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/time/rate"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	armadamaps "github.com/armadaproject/armada/internal/common/maps"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/queue"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	context2 "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/fairness"
	"github.com/armadaproject/armada/pkg/api"
)

// SchedulingAlgo is the interface between the Pulsar-backed scheduler and the
// algorithm deciding which jobs to schedule and preempt.
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes.
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided.
	Schedule(*armadacontext.Context, *jobdb.Txn) (*SchedulerResult, error)
}

// FairSchedulingAlgo is a SchedulingAlgo based on PreemptingQueueScheduler.
type FairSchedulingAlgo struct {
	schedulingConfig            configuration.SchedulingConfig
	executorRepository          database.ExecutorRepository
	queueCache                  queue.QueueCache
	schedulingContextRepository *reports.SchedulingContextRepository
	// Global job scheduling rate-limiter.
	limiter *rate.Limiter
	// Per-queue job scheduling rate-limiters.
	limiterByQueue map[string]*rate.Limiter
	// Max amount of time each scheduling round is allowed to take.
	maxSchedulingDuration time.Duration
	clock                 clock.Clock
	resourceListFactory   *internaltypes.ResourceListFactory
	floatingResourceTypes *floatingresources.FloatingResourceTypes
}

func NewFairSchedulingAlgo(
	config configuration.SchedulingConfig,
	maxSchedulingDuration time.Duration,
	executorRepository database.ExecutorRepository,
	queueCache queue.QueueCache,
	schedulingContextRepository *reports.SchedulingContextRepository,
	resourceListFactory *internaltypes.ResourceListFactory,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
) (*FairSchedulingAlgo, error) {
	if _, ok := config.PriorityClasses[config.DefaultPriorityClassName]; !ok {
		return nil, errors.Errorf(
			"defaultPriorityClassName %s does not correspond to a priority class; priorityClasses is %v",
			config.DefaultPriorityClassName, config.PriorityClasses,
		)
	}
	return &FairSchedulingAlgo{
		schedulingConfig:            config,
		executorRepository:          executorRepository,
		queueCache:                  queueCache,
		schedulingContextRepository: schedulingContextRepository,
		limiter:                     rate.NewLimiter(rate.Limit(config.MaximumSchedulingRate), config.MaximumSchedulingBurst),
		limiterByQueue:              make(map[string]*rate.Limiter),
		maxSchedulingDuration:       maxSchedulingDuration,
		clock:                       clock.RealClock{},
		resourceListFactory:         resourceListFactory,
		floatingResourceTypes:       floatingResourceTypes,
	}, nil
}

// Schedule assigns jobs to nodes in the same way as the old lease call.
// It iterates over each executor in turn (using lexicographical order) and assigns the jobs using a LegacyScheduler, before moving onto the next executor.
// It maintains state of which executors it has considered already and may take multiple Schedule() calls to consider all executors if scheduling is slow.
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *FairSchedulingAlgo) Schedule(
	ctx *armadacontext.Context,
	txn *jobdb.Txn,
) (*SchedulerResult, error) {
	var cancel context.CancelFunc
	if l.maxSchedulingDuration != 0 {
		ctx, cancel = armadacontext.WithTimeout(ctx, l.maxSchedulingDuration)
		defer cancel()
	}
	overallSchedulerResult := &SchedulerResult{
		NodeIdByJobId: make(map[string]string),
	}

	// Exit immediately if scheduling is disabled.
	if l.schedulingConfig.DisableScheduling {
		ctx.Info("scheduling disabled; exiting")
		return overallSchedulerResult, nil
	}

	fsctx, err := l.newFairSchedulingAlgoContext(ctx, txn)
	if err != nil {
		return nil, err
	}

	pools := maps.Keys(fsctx.nodesByPoolAndExecutor)
	sortGroups(pools, l.schedulingConfig.PoolSchedulePriority, l.schedulingConfig.DefaultPoolSchedulePriority)

	ctx.Infof("Looping over pools %s", strings.Join(pools, " "))
	for _, pool := range pools {
		select {
		case <-ctx.Done():
			// We've reached the scheduling time limit; exit gracefully.
			ctx.Info("ending scheduling round early as we have hit the maximum scheduling duration")
			return overallSchedulerResult, nil
		default:
		}
		nodeCountForPool := 0
		for _, executor := range fsctx.executors {
			nodeCountForPool += len(fsctx.nodesByPoolAndExecutor[pool][executor.Id])
		}
		if nodeCountForPool == 0 {
			ctx.Infof("Skipping pool %s as it has no active nodes", pool)
			continue
		}

		ctx.Infof(
			"Scheduling on pool %s with capacity %s", pool, fsctx.totalCapacityByPool[pool].CompactString(),
		)

		start := time.Now()
		schedulerResult, sctx, err := l.schedulePool(
			ctx,
			fsctx,
			pool,
			fsctx.executors,
		)

		ctx.Infof(
			"Scheduled on executor pool %s in %v with error %v",
			pool,
			time.Now().Sub(start),
			err,
		)

		if errors.Is(err, context.DeadlineExceeded) {
			// We've reached the scheduling time limit;
			ctx.Info("stopped scheduling early as we have hit the maximum scheduling duration")
			break
		} else if err != nil {
			return nil, err
		}
		if l.schedulingContextRepository != nil {
			l.schedulingContextRepository.StoreSchedulingContext(sctx)
		}

		preemptedJobs := PreemptedJobsFromSchedulerResult(schedulerResult)
		scheduledJobs := ScheduledJobsFromSchedulerResult(schedulerResult)

		if err := txn.Upsert(preemptedJobs); err != nil {
			return nil, err
		}
		if err := txn.Upsert(scheduledJobs); err != nil {
			return nil, err
		}

		// Aggregate changes across executors.
		overallSchedulerResult.PreemptedJobs = append(overallSchedulerResult.PreemptedJobs, schedulerResult.PreemptedJobs...)
		overallSchedulerResult.ScheduledJobs = append(overallSchedulerResult.ScheduledJobs, schedulerResult.ScheduledJobs...)
		overallSchedulerResult.SchedulingContexts = append(overallSchedulerResult.SchedulingContexts, schedulerResult.SchedulingContexts...)
		maps.Copy(overallSchedulerResult.NodeIdByJobId, schedulerResult.NodeIdByJobId)

		// Update fsctx.
		fsctx.allocationByPoolAndQueueAndPriorityClass[pool] = sctx.AllocatedByQueueAndPriority()
	}
	return overallSchedulerResult, nil
}

type JobQueueIteratorAdapter struct {
	it *immutable.SortedSetIterator[*jobdb.Job]
}

func (it *JobQueueIteratorAdapter) Next() (*jobdb.Job, error) {
	if it.it.Done() {
		return nil, nil
	}
	j, _ := it.it.Next()
	return j, nil
}

type fairSchedulingAlgoContext struct {
	queues                                   []*api.Queue
	priorityFactorByQueue                    map[string]float64
	demandByPoolByQueue                      map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	totalCapacityByPool                      schedulerobjects.QuantityByTAndResourceType[string]
	nodesByPoolAndExecutor                   map[string]map[string][]*schedulerobjects.Node
	jobsByPoolAndExecutor                    map[string]map[string][]*jobdb.Job
	nodeIdByJobId                            map[string]string
	jobIdsByGangId                           map[string]map[string]bool
	gangIdByJobId                            map[string]string
	allocationByPoolAndQueueAndPriorityClass map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	cordonStatusByQueue                      map[string]bool
	executors                                []*schedulerobjects.Executor
	txn                                      *jobdb.Txn
}

func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(ctx *armadacontext.Context, txn *jobdb.Txn) (*fairSchedulingAlgoContext, error) {
	executors, err := l.executorRepository.GetExecutors(ctx)

	nodeById := map[string]*schedulerobjects.Node{}
	executorById := map[string]*schedulerobjects.Executor{}
	nodesByPoolAndExecutor := map[string]map[string][]*schedulerobjects.Node{}
	allKnownPools := map[string]bool{}
	cordonStatusByQueue := make(map[string]bool)

	for _, executor := range executors {
		executorById[executor.Id] = executor
		for _, node := range executor.Nodes {
			nodeById[node.GetId()] = node
			pool := node.GetPool()
			allKnownPools[pool] = true

			if _, present := nodesByPoolAndExecutor[pool]; !present {
				nodesByPoolAndExecutor[pool] = map[string][]*schedulerobjects.Node{}
			}
			if _, present := nodesByPoolAndExecutor[pool][executor.Id]; !present {
				nodesByPoolAndExecutor[pool][executor.Id] = []*schedulerobjects.Node{}
			}
			nodesByPoolAndExecutor[pool][executor.Id] = append(nodesByPoolAndExecutor[pool][executor.Id], node)
		}
	}

	allPools := maps.Keys(allKnownPools)

	if err != nil {
		return nil, err
	}
	executors = l.filterStaleExecutors(ctx, executors)

	queues, err := l.queueCache.GetAll(ctx)
	if err != nil {
		return nil, err
	}
	priorityFactorByQueue := make(map[string]float64)
	for _, queue := range queues {
		priorityFactorByQueue[queue.Name] = float64(queue.PriorityFactor)
		cordonStatusByQueue[queue.Name] = queue.Cordoned
	}

	// Get the total capacity available across executors.
	totalCapacityByPool := make(schedulerobjects.QuantityByTAndResourceType[string])
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			totalCapacityByPool.AddResourceList(node.GetPool(), node.TotalResources)
		}
	}

	for pool, poolCapacity := range totalCapacityByPool {
		poolCapacity.Add(l.floatingResourceTypes.GetTotalAvailableForPool(pool))
	}

	// Create a map of jobs associated with each executor.
	jobsByExecutorId := make(map[string][]*jobdb.Job)
	jobsByPoolAndExecutor := make(map[string]map[string][]*jobdb.Job)
	nodeIdByJobId := make(map[string]string)
	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	demandByPoolByQueue := make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string])

	for _, job := range txn.GetAll() {

		if job.InTerminalState() {
			continue
		}

		// Mark a queue being active for a given pool.  A queue is defined as being active if it has a job running
		// on a pool or if a queued job is eligible for that pool
		pools := job.Pools()

		if !job.Queued() && job.LatestRun() != nil {
			pool := job.LatestRun().Pool()
			pools = []string{pool}
		} else if len(pools) < 1 {
			// This occurs if we haven't assigned a job to a pool.  Right now this can occur if a user
			// has upgraded from a version of armada where pools were not assigned statically.  Eventually we
			// Should be able to remove this
			pools = allPools
		}

		for _, pool := range pools {
			poolQueueResources, ok := demandByPoolByQueue[pool]
			if !ok {
				poolQueueResources = make(map[string]schedulerobjects.QuantityByTAndResourceType[string], len(queues))
				demandByPoolByQueue[pool] = poolQueueResources
			}
			queueResources, ok := poolQueueResources[job.Queue()]
			if !ok {
				queueResources = schedulerobjects.QuantityByTAndResourceType[string]{}
				poolQueueResources[job.Queue()] = queueResources
			}
			// Queued jobs should not be considered for paused queues, so demand := running
			if !cordonStatusByQueue[job.Queue()] || !job.Queued() {
				pcResources, ok := queueResources[job.PriorityClassName()]
				if !ok {
					pcResources = schedulerobjects.NewResourceList(len(job.PodRequirements().ResourceRequirements.Requests))
					queueResources[job.PriorityClassName()] = pcResources
				}
				pcResources.AddV1ResourceList(job.PodRequirements().ResourceRequirements.Requests)
			}
		}

		if job.Queued() {
			continue
		}
		run := job.LatestRun()
		if run == nil {
			continue
		}
		executorId := run.Executor()
		if executorId == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but is not assigned to an executor", run.Id(), job.Id())
		}
		nodeId := run.NodeId()
		if nodeId == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but has no nodeId associated with it", run.Id(), job.Id())
		}
		if nodeName := run.NodeName(); nodeName == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but has no nodeName associated with it", run.Id(), job.Id())
		}
		pool := job.LatestRun().Pool()
		if _, present := jobsByPoolAndExecutor[pool]; !present {
			jobsByPoolAndExecutor[pool] = map[string][]*jobdb.Job{}
		}
		if _, present := jobsByPoolAndExecutor[pool][executorId]; !present {
			jobsByPoolAndExecutor[pool][executorId] = []*jobdb.Job{}
		}
		jobsByPoolAndExecutor[pool][executorId] = append(jobsByPoolAndExecutor[pool][executorId], job)
		jobsByExecutorId[executorId] = append(jobsByExecutorId[executorId], job)
		nodeIdByJobId[job.Id()] = nodeId
		gangInfo, err := context2.GangInfoFromLegacySchedulerJob(job)
		if err != nil {
			return nil, err
		}
		if gangId := gangInfo.Id; gangId != "" {
			jobIds := jobIdsByGangId[gangId]
			if jobIds == nil {
				jobIds = make(map[string]bool)
				jobIdsByGangId[gangId] = jobIds
			}
			jobIds[job.Id()] = true
			gangIdByJobId[job.Id()] = gangId
		}
	}

	// Used to calculate fair share.
	totalAllocationByPoolAndQueue := l.aggregateAllocationByPoolAndQueueAndPriorityClass(executors, jobsByPoolAndExecutor)

	// Filter out any executor that isn't acknowledging jobs in a timely fashion
	// Note that we do this after aggregating allocation across clusters for fair share.
	executors = l.filterLaggingExecutors(ctx, executors, jobsByExecutorId)

	return &fairSchedulingAlgoContext{
		queues:                                   queues,
		priorityFactorByQueue:                    priorityFactorByQueue,
		demandByPoolByQueue:                      demandByPoolByQueue,
		totalCapacityByPool:                      totalCapacityByPool,
		jobsByPoolAndExecutor:                    jobsByPoolAndExecutor,
		nodesByPoolAndExecutor:                   nodesByPoolAndExecutor,
		nodeIdByJobId:                            nodeIdByJobId,
		jobIdsByGangId:                           jobIdsByGangId,
		gangIdByJobId:                            gangIdByJobId,
		cordonStatusByQueue:                      cordonStatusByQueue,
		allocationByPoolAndQueueAndPriorityClass: totalAllocationByPoolAndQueue,
		executors:                                executors,
		txn:                                      txn,
	}, nil
}

// schedulePool schedules jobs on nodes that belong to a given pool.
func (l *FairSchedulingAlgo) schedulePool(
	ctx *armadacontext.Context,
	fsctx *fairSchedulingAlgoContext,
	pool string,
	executors []*schedulerobjects.Executor,
) (*SchedulerResult, *context2.SchedulingContext, error) {
	nodeDb, err := nodedb.NewNodeDb(
		l.schedulingConfig.PriorityClasses,
		l.schedulingConfig.IndexedResources,
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
		l.schedulingConfig.WellKnownNodeTypes,
		l.resourceListFactory,
	)
	if err != nil {
		return nil, nil, err
	}
	for _, executor := range executors {
		jobs := fsctx.jobsByPoolAndExecutor[pool][executor.Id]
		nodes := fsctx.nodesByPoolAndExecutor[pool][executor.Id]
		if err := l.populateNodeDb(nodeDb, jobs, nodes); err != nil {
			return nil, nil, err
		}
	}

	totalResources := fsctx.totalCapacityByPool[pool]
	var fairnessCostProvider fairness.FairnessCostProvider

	// Right now we only support DominantResourceFairness.
	// If we want to support other fairness models it would need to be done here
	fairnessCostProvider, err = fairness.NewDominantResourceFairness(
		totalResources,
		l.schedulingConfig,
	)
	if err != nil {
		return nil, nil, err
	}
	sctx := context2.NewSchedulingContext(
		pool,
		fairnessCostProvider,
		l.limiter,
		totalResources,
	)

	constraints := schedulerconstraints.NewSchedulingConstraints(pool, fsctx.totalCapacityByPool[pool], l.schedulingConfig, fsctx.queues, fsctx.cordonStatusByQueue)

	demandByQueue, ok := fsctx.demandByPoolByQueue[pool]
	if !ok {
		demandByQueue = map[string]schedulerobjects.QuantityByTAndResourceType[string]{}
	}

	for queue, priorityFactor := range fsctx.priorityFactorByQueue {
		demand, hasDemand := demandByQueue[queue]
		if !hasDemand {
			// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
			continue
		}
		cappedDemand := constraints.CapResources(queue, demand)

		var allocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
		if allocatedByQueueAndPriorityClass := fsctx.allocationByPoolAndQueueAndPriorityClass[pool]; allocatedByQueueAndPriorityClass != nil {
			allocatedByPriorityClass = allocatedByQueueAndPriorityClass[queue]
		}
		var weight float64 = 1
		if priorityFactor > 0 {
			weight = 1 / priorityFactor
		}

		// Create per-queue limiters lazily.
		queueLimiter, ok := l.limiterByQueue[queue]
		if !ok {
			queueLimiter = rate.NewLimiter(
				rate.Limit(l.schedulingConfig.MaximumPerQueueSchedulingRate),
				l.schedulingConfig.MaximumPerQueueSchedulingBurst,
			)
			l.limiterByQueue[queue] = queueLimiter
		}

		if err := sctx.AddQueueSchedulingContext(queue, weight, allocatedByPriorityClass, demand.AggregateByResource(), cappedDemand.AggregateByResource(), queueLimiter); err != nil {
			return nil, nil, err
		}
	}

	sctx.UpdateFairShares()
	scheduler := NewPreemptingQueueScheduler(
		sctx,
		constraints,
		l.floatingResourceTypes,
		l.schedulingConfig.ProtectedFractionOfFairShare,
		fsctx.txn,
		nodeDb,
		fsctx.nodeIdByJobId,
		fsctx.jobIdsByGangId,
		fsctx.gangIdByJobId,
	)
	result, err := scheduler.Schedule(ctx)
	if err != nil {
		return nil, nil, err
	}
	for i, jctx := range result.PreemptedJobs {
		jobDbJob := jctx.Job
		now := l.clock.Now()
		if run := jobDbJob.LatestRun(); run != nil {
			jobDbJob = jobDbJob.WithUpdatedRun(run.WithFailed(true).WithPreemptedTime(&now))
		} else {
			return nil, nil, errors.Errorf("attempting to preempt job %s with no associated runs", jobDbJob.Id())
		}
		result.PreemptedJobs[i].Job = jobDbJob.WithQueued(false).WithFailed(true)
	}
	for i, jctx := range result.ScheduledJobs {
		jobDbJob := jctx.Job
		jobId := jobDbJob.Id()
		nodeId := result.NodeIdByJobId[jobId]
		if nodeId == "" {
			return nil, nil, errors.Errorf("job %s not mapped to a node", jobId)
		}
		node, err := nodeDb.GetNode(nodeId)
		if err != nil {
			return nil, nil, err
		}
		priority, ok := nodeDb.GetScheduledAtPriority(jobId)
		if !ok {
			return nil, nil, errors.Errorf("job %s not mapped to a priority", jobId)
		}
		result.ScheduledJobs[i].Job = jobDbJob.
			WithQueuedVersion(jobDbJob.QueuedVersion()+1).
			WithQueued(false).
			WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), priority)
	}
	return result, sctx, nil
}

// populateNodeDb adds all the nodes and jobs associated with a particular pool to the nodeDb.
func (l *FairSchedulingAlgo) populateNodeDb(nodeDb *nodedb.NodeDb, jobs []*jobdb.Job, nodes []*schedulerobjects.Node) error {
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	nodesById := armadaslices.GroupByFuncUnique(
		nodes,
		func(node *schedulerobjects.Node) string { return node.Id },
	)
	jobsByNodeId := make(map[string][]*jobdb.Job, len(nodes))
	for _, job := range jobs {
		if job.InTerminalState() || !job.HasRuns() {
			continue
		}
		nodeId := job.LatestRun().NodeId()
		if _, ok := nodesById[nodeId]; !ok {
			logrus.Errorf(
				"job %s assigned to node %s on executor %s, but no such node found",
				job.Id(), nodeId, job.LatestRun().Executor(),
			)
			continue
		}
		jobsByNodeId[nodeId] = append(jobsByNodeId[nodeId], job)
	}

	for _, node := range nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobsByNodeId[node.Id], node); err != nil {
			return err
		}
	}
	txn.Commit()
	return nil
}

// filterStaleExecutors returns all executors which have sent a lease request within the duration given by l.schedulingConfig.ExecutorTimeout.
// This ensures that we don't continue to assign jobs to executors that are no longer active.
func (l *FairSchedulingAlgo) filterStaleExecutors(ctx *armadacontext.Context, executors []*schedulerobjects.Executor) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	cutoff := l.clock.Now().Add(-l.schedulingConfig.ExecutorTimeout)
	for _, executor := range executors {
		if executor.LastUpdateTime.After(cutoff) {
			activeExecutors = append(activeExecutors, executor)
		} else {
			ctx.Infof("Ignoring executor %s because it hasn't heartbeated since %s", executor.Id, executor.LastUpdateTime)
		}
	}
	return activeExecutors
}

// filterLaggingExecutors returns all executors with <= l.schedulingConfig.MaxUnacknowledgedJobsPerExecutor unacknowledged jobs,
// where unacknowledged means the executor has not echoed the job since it was scheduled.
//
// Used to rate-limit scheduling onto executors that can't keep up.
//
// TODO: Let's also check that jobs are on the right nodes.
func (l *FairSchedulingAlgo) filterLaggingExecutors(
	ctx *armadacontext.Context,
	executors []*schedulerobjects.Executor,
	leasedJobsByExecutor map[string][]*jobdb.Job,
) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	for _, executor := range executors {
		leasedJobs := leasedJobsByExecutor[executor.Id]
		executorRuns, err := executor.AllRuns()
		if err != nil {
			logging.
				WithStacktrace(ctx, err).
				Errorf("failed to retrieve runs for executor %s; will not be considered for scheduling", executor.Id)
			continue
		}
		executorRunIds := make(map[string]bool, len(executorRuns))
		for _, run := range executorRuns {
			executorRunIds[run] = true
		}

		var numUnacknowledgedJobs uint
		for _, leasedJob := range leasedJobs {
			if leasedJob.HasRuns() && !leasedJob.InTerminalState() {
				if !executorRunIds[leasedJob.LatestRun().Id()] {
					numUnacknowledgedJobs++
				}
			}
		}
		if numUnacknowledgedJobs <= l.schedulingConfig.MaxUnacknowledgedJobsPerExecutor {
			activeExecutors = append(activeExecutors, executor)
		} else {
			ctx.Warnf(
				"%d unacknowledged jobs on executor %s exceeds limit of %d; executor will not be considered for scheduling",
				numUnacknowledgedJobs, executor.Id, l.schedulingConfig.MaxUnacknowledgedJobsPerExecutor,
			)
		}
	}
	return activeExecutors
}

func (l *FairSchedulingAlgo) aggregateAllocationByPoolAndQueueAndPriorityClass(
	activeExecutors []*schedulerobjects.Executor,
	jobsByPoolAndExecutor map[string]map[string][]*jobdb.Job,
) map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string] {
	rv := make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string])
	activeExecutorsSet := armadamaps.FromSlice(activeExecutors,
		func(e *schedulerobjects.Executor) string {
			return e.Id
		}, func(e *schedulerobjects.Executor) bool {
			return true
		})

	for pool, executors := range jobsByPoolAndExecutor {
		allocationByQueue := rv[pool]
		if allocationByQueue == nil {
			allocationByQueue = make(map[string]schedulerobjects.QuantityByTAndResourceType[string])
			rv[pool] = allocationByQueue
		}
		for executorId, jobs := range executors {
			if _, isActive := activeExecutorsSet[executorId]; !isActive {
				continue
			}
			for _, job := range jobs {
				queue := job.Queue()
				allocation := allocationByQueue[queue]
				if allocation == nil {
					allocation = make(schedulerobjects.QuantityByTAndResourceType[string])
					allocationByQueue[queue] = allocation
				}
				allocation.AddV1ResourceList(job.PriorityClassName(), job.ResourceRequirements().Requests)
			}
		}
	}
	return rv
}

// sortGroups sorts the given list of groups based on priorities defined in groupToPriority map.
// If a group's priority is not specified in the map, the defaultPriority is used. The groups are primarily
// sorted by descending priority. If two groups have the same priority, they are sorted alphabetically by their names.
func sortGroups(groups []string, groupToPriority map[string]int, defaultPriority int) {
	if groupToPriority == nil {
		groupToPriority = map[string]int{}
	}
	// Sort the groups using a custom comparison function
	sort.Slice(groups, func(i, j int) bool {
		// Retrieve or default the priority for the i-th group
		priI, okI := groupToPriority[groups[i]]
		if !okI {
			priI = defaultPriority
		}
		// Retrieve or default the priority for the j-th group
		priJ, okJ := groupToPriority[groups[j]]
		if !okJ {
			priJ = defaultPriority
		}
		// Sort primarily by priority (descending)
		if priI != priJ {
			return priI > priJ
		}
		// If priorities are equal, sort by name (ascending)
		return groups[i] < groups[j]
	})
}
