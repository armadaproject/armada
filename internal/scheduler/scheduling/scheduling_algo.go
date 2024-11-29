package scheduling

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"golang.org/x/time/rate"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
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
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/scheduling/context"
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
		NodeIdByJobId:          make(map[string]string),
		PerPoolSchedulingStats: make(map[string]PerPoolSchedulingStats),
	}

	// Exit immediately if scheduling is disabled.
	if l.schedulingConfig.DisableScheduling {
		ctx.Info("scheduling disabled; exiting")
		return overallSchedulerResult, nil
	}

	for _, pool := range l.schedulingConfig.Pools {
		select {
		case <-ctx.Done():
			// We've reached the scheduling time limit; exit gracefully.
			ctx.Info("ending scheduling round early as we have hit the maximum scheduling duration")
			return overallSchedulerResult, nil
		default:
		}

		fsctx, err := l.newFairSchedulingAlgoContext(ctx, txn, pool)
		if err != nil {
			return nil, err
		}

		if fsctx.nodeDb.NumNodes() <= 0 {
			ctx.Infof("Skipping pool %s as it has no active nodes", pool.Name)
			continue
		}

		start := time.Now()
		schedulerResult, sctx, err := l.SchedulePool(ctx, fsctx, pool.Name)

		ctx.Infof("Scheduled on executor pool %s in %v with error %v", pool, time.Now().Sub(start), err)

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

		for p, s := range schedulerResult.PerPoolSchedulingStats {
			overallSchedulerResult.PerPoolSchedulingStats[p] = s
		}
	}
	return overallSchedulerResult, nil
}

type FairSchedulingAlgoContext struct {
	queues            map[string]*api.Queue
	pool              string
	nodeDb            *nodedb.NodeDb
	schedulingContext *schedulercontext.SchedulingContext
	nodeIdByJobId     map[string]string
	jobIdsByGangId    map[string]map[string]bool
	gangIdByJobId     map[string]string
	Txn               *jobdb.Txn
}

func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(ctx *armadacontext.Context, txn *jobdb.Txn, pool configuration.PoolConfig) (*FairSchedulingAlgoContext, error) {
	executors, err := l.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}

	queues, err := l.queueCache.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	queueByName := armadamaps.FromSlice(queues,
		func(queue *api.Queue) string { return queue.Name },
		func(queue *api.Queue) *api.Queue { return queue })

	awayAllocationPools := []string{}
	for _, otherPool := range l.schedulingConfig.Pools {
		if slices.Contains(otherPool.AwayPools, pool.Name) {
			awayAllocationPools = append(awayAllocationPools, otherPool.Name)
		}
	}
	allPools := []string{pool.Name}
	allPools = append(allPools, pool.AwayPools...)
	allPools = append(allPools, awayAllocationPools...)

	jobSchedulingInfo, err := calculateJobSchedulingInfo(ctx,
		armadamaps.FromSlice(executors,
			func(ex *schedulerobjects.Executor) string { return ex.Id },
			func(_ *schedulerobjects.Executor) bool { return true }),
		queueByName,
		txn.GetAll(),
		pool.Name,
		awayAllocationPools,
		allPools)
	if err != nil {
		return nil, err
	}

	nodeFactory := internaltypes.NewNodeFactory(
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
		l.resourceListFactory,
	)

	// Filter out any executor that isn't acknowledging jobs in a timely fashion
	// Note that we do this after aggregating allocation across clusters for fair share.
	healthyExecutors := l.filterStaleExecutors(ctx, executors)
	healthyExecutors = l.filterLaggingExecutors(ctx, healthyExecutors, jobSchedulingInfo.jobsByExecutorId)
	if l.schedulingConfig.EnableExecutorCordoning {
		executorSettings, err := l.executorRepository.GetExecutorSettings(ctx)
		if err != nil {
			return nil, err
		}
		healthyExecutors = l.filterCordonedExecutors(ctx, healthyExecutors, executorSettings)
	}

	nodes := nodeFactory.FromSchedulerObjectsExecutors(healthyExecutors, func(errMes string) {
		ctx.Error(errMes)
	})

	homeJobs := jobSchedulingInfo.jobsByPool[pool.Name]
	awayJobs := []*jobdb.Job{}

	for _, otherPool := range l.schedulingConfig.Pools {
		if pool.Name == otherPool.Name {
			continue
		}
		if slices.Contains(otherPool.AwayPools, pool.Name) {
			homeJobs = append(homeJobs, jobSchedulingInfo.jobsByPool[otherPool.Name]...)
		}
	}

	for _, awayPool := range pool.AwayPools {
		awayJobs = append(awayJobs, jobSchedulingInfo.jobsByPool[awayPool]...)
	}

	nodePools := append(pool.AwayPools, pool.Name)

	nodeDb, err := l.constructNodeDb(homeJobs, awayJobs,
		armadaslices.Filter(nodes, func(node *internaltypes.Node) bool { return slices.Contains(nodePools, node.GetPool()) }))
	if err != nil {
		return nil, err
	}

	totalResources := nodeDb.TotalKubernetesResources()
	totalResources = totalResources.Add(l.floatingResourceTypes.GetTotalAvailableForPool(pool.Name))

	schedulingContext, err := l.constructSchedulingContext(
		pool.Name,
		totalResources,
		jobSchedulingInfo.demandByQueueAndPriorityClass,
		jobSchedulingInfo.allocatedByQueueAndPriorityClass,
		jobSchedulingInfo.awayAllocatedByQueueAndPriorityClass,
		queueByName)
	if err != nil {
		return nil, err
	}

	return &FairSchedulingAlgoContext{
		queues:            queueByName,
		pool:              pool.Name,
		nodeDb:            nodeDb,
		schedulingContext: schedulingContext,
		nodeIdByJobId:     jobSchedulingInfo.nodeIdByJobId,
		jobIdsByGangId:    jobSchedulingInfo.jobIdsByGangId,
		gangIdByJobId:     jobSchedulingInfo.gangIdByJobId,
		Txn:               txn,
	}, nil
}

type jobSchedulingInfo struct {
	jobsByExecutorId                     map[string][]*jobdb.Job
	jobsByPool                           map[string][]*jobdb.Job
	nodeIdByJobId                        map[string]string
	jobIdsByGangId                       map[string]map[string]bool
	gangIdByJobId                        map[string]string
	demandByQueueAndPriorityClass        map[string]map[string]internaltypes.ResourceList
	allocatedByQueueAndPriorityClass     map[string]map[string]internaltypes.ResourceList
	awayAllocatedByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList
}

func calculateJobSchedulingInfo(ctx *armadacontext.Context, activeExecutorsSet map[string]bool,
	queues map[string]*api.Queue, jobs []*jobdb.Job, currentPool string, awayAllocationPools []string, allPools []string,
) (*jobSchedulingInfo, error) {
	jobsByExecutorId := make(map[string][]*jobdb.Job)
	jobsByPool := make(map[string][]*jobdb.Job)
	nodeIdByJobId := make(map[string]string)
	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	demandByQueueAndPriorityClass := make(map[string]map[string]internaltypes.ResourceList)
	allocatedByQueueAndPriorityClass := make(map[string]map[string]internaltypes.ResourceList)
	awayAllocatedByQueueAndPriorityClass := make(map[string]map[string]internaltypes.ResourceList)

	for _, job := range jobs {
		if job.InTerminalState() {
			continue
		}

		queue, present := queues[job.Queue()]
		if !present {
			ctx.Errorf("job %s is running with queue %s, queue does not exist", job.Id(), job.Queue())
			continue
		}

		// Mark a queue being active for a given pool.  A queue is defined as being active if it has a job running
		// on a pool or if a queued job is eligible for that pool
		pools := job.Pools()

		if !job.Queued() && job.LatestRun() != nil {
			pool := job.LatestRun().Pool()
			pools = []string{pool}
		}

		matches := false
		for _, pool := range pools {
			if slices.Contains(allPools, pool) {
				matches = true
				break
			}
		}
		if !matches {
			continue
		}

		if slices.Contains(pools, currentPool) {
			queueResources, ok := demandByQueueAndPriorityClass[job.Queue()]
			if !ok {
				queueResources = map[string]internaltypes.ResourceList{}
				demandByQueueAndPriorityClass[job.Queue()] = queueResources
			}
			// Queued jobs should not be considered for paused queues, so demand := running
			if !queue.Cordoned || !job.Queued() {
				pcName := job.PriorityClassName()
				queueResources[pcName] = queueResources[pcName].Add(job.AllResourceRequirements())
			}
		}

		if job.Queued() || job.LatestRun() == nil {
			continue
		}
		run := job.LatestRun()
		executorId := run.Executor()
		if executorId == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but is not assigned to an executor", run.Id(), job.Id())
		}
		nodeId := run.NodeId()
		if nodeId == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but has no nodeId associated with it", run.Id(), job.Id())
		}

		pool := job.LatestRun().Pool()

		if _, isActive := activeExecutorsSet[executorId]; isActive {
			if pool == currentPool {
				allocation := allocatedByQueueAndPriorityClass[queue.Name]
				if allocation == nil {
					allocation = make(map[string]internaltypes.ResourceList)
					allocatedByQueueAndPriorityClass[queue.Name] = allocation
				}
				allocation[job.PriorityClassName()] = allocation[job.PriorityClassName()].Add(job.AllResourceRequirements())
			} else if slices.Contains(awayAllocationPools, pool) {
				awayAllocation := awayAllocatedByQueueAndPriorityClass[queue.Name]
				if awayAllocation == nil {
					awayAllocation = make(map[string]internaltypes.ResourceList)
					awayAllocatedByQueueAndPriorityClass[queue.Name] = awayAllocation
				}
				awayAllocation[job.PriorityClassName()] = awayAllocation[job.PriorityClassName()].Add(job.AllResourceRequirements())
			}
		}
		if _, present := jobsByPool[pool]; !present {
			jobsByPool[pool] = []*jobdb.Job{}
		}
		jobsByPool[pool] = append(jobsByPool[pool], job)
		jobsByExecutorId[executorId] = append(jobsByExecutorId[executorId], job)
		nodeIdByJobId[job.Id()] = nodeId
		gangInfo, err := schedulercontext.GangInfoFromLegacySchedulerJob(job)
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

	return &jobSchedulingInfo{
		jobsByExecutorId:                     jobsByExecutorId,
		jobsByPool:                           jobsByPool,
		nodeIdByJobId:                        nodeIdByJobId,
		jobIdsByGangId:                       jobIdsByGangId,
		gangIdByJobId:                        gangIdByJobId,
		demandByQueueAndPriorityClass:        demandByQueueAndPriorityClass,
		allocatedByQueueAndPriorityClass:     allocatedByQueueAndPriorityClass,
		awayAllocatedByQueueAndPriorityClass: awayAllocatedByQueueAndPriorityClass,
	}, nil
}

func (l *FairSchedulingAlgo) constructNodeDb(homeJobs []*jobdb.Job, awayJobs []*jobdb.Job, nodes []*internaltypes.Node) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		l.schedulingConfig.PriorityClasses,
		l.schedulingConfig.IndexedResources,
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
		l.schedulingConfig.WellKnownNodeTypes,
		l.resourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	if err := l.populateNodeDb(nodeDb, homeJobs, awayJobs, nodes); err != nil {
		return nil, err
	}

	return nodeDb, nil
}

func (l *FairSchedulingAlgo) constructSchedulingContext(
	pool string,
	totalCapacity internaltypes.ResourceList,
	demandByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList,
	allocationByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList,
	awayAllocationByQueueAndPriorityClass map[string]map[string]internaltypes.ResourceList,
	queues map[string]*api.Queue,
) (*schedulercontext.SchedulingContext, error) {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalCapacity, l.schedulingConfig)
	if err != nil {
		return nil, err
	}
	sctx := schedulercontext.NewSchedulingContext(pool, fairnessCostProvider, l.limiter, totalCapacity)
	constraints := schedulerconstraints.NewSchedulingConstraints(pool, totalCapacity, l.schedulingConfig, maps.Values(queues))

	for _, queue := range queues {
		demand, hasDemand := demandByQueueAndPriorityClass[queue.Name]
		if !hasDemand {
			// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
			continue
		}
		cappedDemand := constraints.CapResources(queue.Name, demand)

		var allocatedByPriorityClass map[string]internaltypes.ResourceList
		if allocationByQueueAndPriorityClass != nil {
			allocatedByPriorityClass = allocationByQueueAndPriorityClass[queue.Name]
		}
		var weight float64 = 1
		if queue.PriorityFactor > 0 {
			weight = 1 / queue.PriorityFactor
		}

		queueLimiter, ok := l.limiterByQueue[queue.Name]
		if !ok {
			queueLimiter = rate.NewLimiter(
				rate.Limit(l.schedulingConfig.MaximumPerQueueSchedulingRate),
				l.schedulingConfig.MaximumPerQueueSchedulingBurst,
			)
			l.limiterByQueue[queue.Name] = queueLimiter
		}

		if err := sctx.AddQueueSchedulingContext(queue.Name, weight, allocatedByPriorityClass, internaltypes.RlMapSumValues(demand), internaltypes.RlMapSumValues(cappedDemand), queueLimiter); err != nil {
			return nil, err
		}
	}

	for _, queue := range queues {
		allocation, hasAllocation := awayAllocationByQueueAndPriorityClass[queue.Name]
		if !hasAllocation {
			// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
			continue
		}

		var weight float64 = 1
		if queue.PriorityFactor > 0 {
			weight = 1 / queue.PriorityFactor
		}

		if err := sctx.AddQueueSchedulingContext(schedulercontext.CalculateAwayQueueName(queue.Name), weight, allocation, internaltypes.ResourceList{}, internaltypes.ResourceList{}, nil); err != nil {
			return nil, err
		}
	}

	sctx.UpdateFairShares()

	return sctx, nil
}

// SchedulePool schedules jobs on nodes that belong to a given pool.
func (l *FairSchedulingAlgo) SchedulePool(
	ctx *armadacontext.Context,
	fsctx *FairSchedulingAlgoContext,
	pool string,
) (*SchedulerResult, *schedulercontext.SchedulingContext, error) {
	totalResources := fsctx.nodeDb.TotalKubernetesResources()
	totalResources = totalResources.Add(l.floatingResourceTypes.GetTotalAvailableForPool(pool))

	constraints := schedulerconstraints.NewSchedulingConstraints(pool, totalResources, l.schedulingConfig, maps.Values(fsctx.queues))

	protectedFractionOfFairShare := l.schedulingConfig.GetProtectedFractionOfFairShare(pool)
	scheduler := NewPreemptingQueueScheduler(
		fsctx.schedulingContext,
		constraints,
		l.floatingResourceTypes,
		l.schedulingConfig.EnablePreferLargeJobOrdering,
		protectedFractionOfFairShare,
		l.schedulingConfig.MaxQueueLookback,
		fsctx.Txn,
		fsctx.nodeDb,
		fsctx.nodeIdByJobId,
		fsctx.jobIdsByGangId,
		fsctx.gangIdByJobId,
	)

	ctx.Infof("Scheduling on pool %s with capacity %s protectedFractionOfFairShare %f",
		pool,
		fsctx.nodeDb.TotalKubernetesResources().Add(l.floatingResourceTypes.GetTotalAvailableForPool(pool)).String(),
		protectedFractionOfFairShare,
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
		node, err := fsctx.nodeDb.GetNode(nodeId)
		if err != nil {
			return nil, nil, err
		}
		priority, ok := fsctx.nodeDb.GetScheduledAtPriority(jobId)
		if !ok {
			return nil, nil, errors.Errorf("job %s not mapped to a priority", jobId)
		}
		result.ScheduledJobs[i].Job = jobDbJob.
			WithQueuedVersion(jobDbJob.QueuedVersion()+1).
			WithQueued(false).
			WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), pool, priority)
	}
	return result, fsctx.schedulingContext, nil
}

// populateNodeDb adds all the nodes and jobs associated with a particular pool to the nodeDb.
func (l *FairSchedulingAlgo) populateNodeDb(nodeDb *nodedb.NodeDb, homeJobs []*jobdb.Job, awayJobs []*jobdb.Job, nodes []*internaltypes.Node) error {
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	nodesById := armadaslices.GroupByFuncUnique(
		nodes,
		func(node *internaltypes.Node) string { return node.GetId() },
	)
	jobsByNodeId := make(map[string][]*jobdb.Job, len(nodes))
	for _, job := range homeJobs {
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
	for _, job := range awayJobs {
		if job.InTerminalState() || !job.HasRuns() {
			continue
		}
		nodeId := job.LatestRun().NodeId()
		node, ok := nodesById[nodeId]
		if !ok {
			logrus.Errorf(
				"job %s assigned to node %s on executor %s, but no such node found",
				job.Id(), nodeId, job.LatestRun().Executor(),
			)
			continue
		}

		markResourceUnallocatable(node.AllocatableByPriority, job.KubernetesResourceRequirements())
	}

	for _, node := range nodes {
		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobsByNodeId[node.GetId()], node); err != nil {
			return err
		}
	}
	txn.Commit()
	return nil
}

func markResourceUnallocatable(allocatableByPriority map[int32]internaltypes.ResourceList, rl internaltypes.ResourceList) {
	for pri, allocatable := range allocatableByPriority {
		newAllocatable := allocatable.Subtract(rl).FloorAtZero()
		allocatableByPriority[pri] = newAllocatable
	}
}

// filterCordonedExecutors returns all executors which aren't marked as cordoned from the provided executorSettings
func (l *FairSchedulingAlgo) filterCordonedExecutors(ctx *armadacontext.Context, executors []*schedulerobjects.Executor, executorSettings []*schedulerobjects.ExecutorSettings) []*schedulerobjects.Executor {
	settingsMap := map[string]*schedulerobjects.ExecutorSettings{}
	for _, es := range executorSettings {
		settingsMap[es.ExecutorId] = es
	}
	notCordoned := func(e *schedulerobjects.Executor) bool {
		settings, ok := settingsMap[e.Id]
		cordoned := ok && settings.Cordoned
		if cordoned {
			ctx.Infof("Ignoring executor %s as it was cordoned for reason: %s", e.Id, settings.CordonReason)
		}
		return !cordoned
	}
	return armadaslices.Filter(executors, notCordoned)
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
func (l *FairSchedulingAlgo) filterLaggingExecutors(
	ctx *armadacontext.Context,
	executors []*schedulerobjects.Executor,
	leasedJobsByExecutor map[string][]*jobdb.Job,
) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	for _, executor := range executors {
		leasedJobs := leasedJobsByExecutor[executor.Id]
		executorRuns := executor.AllRuns()
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
