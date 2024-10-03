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

	pools := maps.Keys(fsctx.pools)
	sortGroups(pools, l.schedulingConfig.PoolSchedulePriority, l.schedulingConfig.DefaultPoolSchedulePriority)

	ctx.Infof("Looping over pools %s", strings.Join(pools, " "))
	for _, poolName := range pools {
		select {
		case <-ctx.Done():
			// We've reached the scheduling time limit; exit gracefully.
			ctx.Info("ending scheduling round early as we have hit the maximum scheduling duration")
			return overallSchedulerResult, nil
		default:
		}
		pool, present := fsctx.pools[poolName]
		if !present {
			ctx.Errorf("Skipping pool %s could not find pool scheduling context with that name", poolName)
			continue
		}
		if pool.nodeDb.NumNodes() <= 0 {
			ctx.Infof("Skipping pool %s as it has no active nodes", poolName)
			continue
		}

		ctx.Infof("Scheduling on pool %s with capacity %s", pool, pool.totalCapacity.CompactString())

		start := time.Now()
		schedulerResult, sctx, err := l.schedulePool(ctx, fsctx, poolName)

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
	queues         map[string]*api.Queue
	pools          map[string]*poolSchedulingInfo
	nodeIdByJobId  map[string]string
	jobIdsByGangId map[string]map[string]bool
	gangIdByJobId  map[string]string
	txn            *jobdb.Txn
}

type poolSchedulingInfo struct {
	name              string
	nodeDb            *nodedb.NodeDb
	schedulingContext *schedulercontext.SchedulingContext
	totalCapacity     schedulerobjects.ResourceList
}

func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(ctx *armadacontext.Context, txn *jobdb.Txn) (*fairSchedulingAlgoContext, error) {
	executors, err := l.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}
	executors = l.filterStaleExecutors(ctx, executors)

	queues, err := l.queueCache.GetAll(ctx)
	if err != nil {
		return nil, err
	}

	queueByName := armadamaps.FromSlice(queues,
		func(queue *api.Queue) string { return queue.Name },
		func(queue *api.Queue) *api.Queue { return queue })

	allKnownPools := map[string]bool{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			allKnownPools[node.GetPool()] = true
		}
	}

	jobSchedulingInfo, err := calculateJobSchedulingInfo(ctx, executors, queueByName, txn.GetAll(), maps.Keys(allKnownPools))
	if err != nil {
		return nil, err
	}

	// Filter out any executor that isn't acknowledging jobs in a timely fashion
	// Note that we do this after aggregating allocation across clusters for fair share.
	healthyExecutors := l.filterLaggingExecutors(ctx, executors, jobSchedulingInfo.jobsByExecutorId)
	nodes := []*schedulerobjects.Node{}
	for _, executor := range healthyExecutors {
		nodes = append(nodes, executor.Nodes...)
	}

	pools := make(map[string]*poolSchedulingInfo, len(allKnownPools))
	for _, pool := range maps.Keys(allKnownPools) {
		nodeDb, err := l.constructNodeDb(jobSchedulingInfo.jobsByPool[pool], armadaslices.Filter(nodes, func(node *schedulerobjects.Node) bool { return node.Pool == pool }))
		if err != nil {
			return nil, err
		}

		schedulingContext, err := l.constructSchedulingContext(
			pool,
			l.getCapacityForPool(pool, executors),
			jobSchedulingInfo.demandByPoolByQueue[pool],
			jobSchedulingInfo.allocatedByPoolAndQueueAndPriorityClass[pool],
			queueByName)
		if err != nil {
			return nil, err
		}

		pools[pool] = &poolSchedulingInfo{
			name:              pool,
			nodeDb:            nodeDb,
			schedulingContext: schedulingContext,
			totalCapacity:     l.getCapacityForPool(pool, executors),
		}
	}

	return &fairSchedulingAlgoContext{
		queues:         queueByName,
		pools:          pools,
		nodeIdByJobId:  jobSchedulingInfo.nodeIdByJobId,
		jobIdsByGangId: jobSchedulingInfo.jobIdsByGangId,
		gangIdByJobId:  jobSchedulingInfo.gangIdByJobId,
		txn:            txn,
	}, nil
}

func (l *FairSchedulingAlgo) getCapacityForPool(pool string, executors []*schedulerobjects.Executor) schedulerobjects.ResourceList {
	totalCapacity := schedulerobjects.ResourceList{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			if node.Pool == pool {
				totalCapacity.Add(node.TotalResources)
			}
		}
	}
	totalCapacity.Add(l.floatingResourceTypes.GetTotalAvailableForPool(pool))
	return totalCapacity
}

type jobSchedulingInfo struct {
	jobsByExecutorId                        map[string][]*jobdb.Job
	jobsByPool                              map[string][]*jobdb.Job
	nodeIdByJobId                           map[string]string
	jobIdsByGangId                          map[string]map[string]bool
	gangIdByJobId                           map[string]string
	demandByPoolByQueue                     map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	allocatedByPoolAndQueueAndPriorityClass map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
}

func calculateJobSchedulingInfo(ctx *armadacontext.Context, executors []*schedulerobjects.Executor,
	queues map[string]*api.Queue, jobs []*jobdb.Job, allPools []string,
) (*jobSchedulingInfo, error) {
	activeExecutorsSet := map[string]bool{}
	for _, executor := range executors {
		activeExecutorsSet[executor.Id] = true
	}

	jobsByExecutorId := make(map[string][]*jobdb.Job)
	jobsByPool := make(map[string][]*jobdb.Job)
	nodeIdByJobId := make(map[string]string)
	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	demandByPoolByQueue := make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string])
	allocatedByPoolAndQueueAndPriorityClass := make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string])

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
			if !queue.Cordoned || !job.Queued() {
				pcResources, ok := queueResources[job.PriorityClassName()]
				if !ok {
					pcResources = schedulerobjects.NewResourceList(len(job.PodRequirements().ResourceRequirements.Requests))
					queueResources[job.PriorityClassName()] = pcResources
				}
				pcResources.AddV1ResourceList(job.PodRequirements().ResourceRequirements.Requests)
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
		allocationByQueue := allocatedByPoolAndQueueAndPriorityClass[pool]
		if allocationByQueue == nil {
			allocationByQueue = make(map[string]schedulerobjects.QuantityByTAndResourceType[string])
			allocatedByPoolAndQueueAndPriorityClass[pool] = allocationByQueue
		}
		if _, isActive := activeExecutorsSet[executorId]; isActive {
			allocation := allocationByQueue[queue.Name]
			if allocation == nil {
				allocation = make(schedulerobjects.QuantityByTAndResourceType[string])
				allocationByQueue[queue.Name] = allocation
			}
			allocation.AddV1ResourceList(job.PriorityClassName(), job.ResourceRequirements().Requests)
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
		jobsByExecutorId:                        jobsByExecutorId,
		jobsByPool:                              jobsByPool,
		nodeIdByJobId:                           nodeIdByJobId,
		jobIdsByGangId:                          jobIdsByGangId,
		gangIdByJobId:                           gangIdByJobId,
		demandByPoolByQueue:                     demandByPoolByQueue,
		allocatedByPoolAndQueueAndPriorityClass: allocatedByPoolAndQueueAndPriorityClass,
	}, nil
}

func (l *FairSchedulingAlgo) constructNodeDb(jobs []*jobdb.Job, nodes []*schedulerobjects.Node) (*nodedb.NodeDb, error) {
	nodeFactory := internaltypes.NewNodeFactory(
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
		l.resourceListFactory,
	)

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
	if err := l.populateNodeDb(nodeDb, nodeFactory, jobs, nodes); err != nil {
		return nil, err
	}

	return nodeDb, nil
}

func (l *FairSchedulingAlgo) constructSchedulingContext(
	pool string,
	totalCapacity schedulerobjects.ResourceList,
	demandByQueue map[string]schedulerobjects.QuantityByTAndResourceType[string],
	allocationByQueueAndPriorityClass map[string]schedulerobjects.QuantityByTAndResourceType[string],
	queues map[string]*api.Queue,
) (*schedulercontext.SchedulingContext, error) {
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(totalCapacity, l.schedulingConfig)
	if err != nil {
		return nil, err
	}
	sctx := schedulercontext.NewSchedulingContext(pool, fairnessCostProvider, l.limiter, totalCapacity)
	constraints := schedulerconstraints.NewSchedulingConstraints(pool, totalCapacity, l.schedulingConfig, maps.Values(queues))

	for _, queue := range queues {
		demand, hasDemand := demandByQueue[queue.Name]
		if !hasDemand {
			// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
			continue
		}
		cappedDemand := constraints.CapResources(queue.Name, demand)

		var allocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
		if allocatedByQueueAndPriorityClass := allocationByQueueAndPriorityClass; allocatedByQueueAndPriorityClass != nil {
			allocatedByPriorityClass = allocatedByQueueAndPriorityClass[queue.Name]
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

		if err := sctx.AddQueueSchedulingContext(queue.Name, weight, allocatedByPriorityClass, demand.AggregateByResource(), cappedDemand.AggregateByResource(), queueLimiter); err != nil {
			return nil, err
		}
	}

	sctx.UpdateFairShares()

	return sctx, nil
}

// schedulePool schedules jobs on nodes that belong to a given pool.
func (l *FairSchedulingAlgo) schedulePool(
	ctx *armadacontext.Context,
	fsctx *fairSchedulingAlgoContext,
	pool string,
) (*SchedulerResult, *schedulercontext.SchedulingContext, error) {
	poolContext := fsctx.pools[pool]

	constraints := schedulerconstraints.NewSchedulingConstraints(pool, poolContext.totalCapacity, l.schedulingConfig, maps.Values(fsctx.queues))

	scheduler := NewPreemptingQueueScheduler(
		poolContext.schedulingContext,
		constraints,
		l.floatingResourceTypes,
		l.schedulingConfig.ProtectedFractionOfFairShare,
		fsctx.txn,
		poolContext.nodeDb,
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
		node, err := poolContext.nodeDb.GetNode(nodeId)
		if err != nil {
			return nil, nil, err
		}
		priority, ok := poolContext.nodeDb.GetScheduledAtPriority(jobId)
		if !ok {
			return nil, nil, errors.Errorf("job %s not mapped to a priority", jobId)
		}
		result.ScheduledJobs[i].Job = jobDbJob.
			WithQueuedVersion(jobDbJob.QueuedVersion()+1).
			WithQueued(false).
			WithNewRun(node.GetExecutor(), node.GetId(), node.GetName(), node.GetPool(), priority)
	}
	return result, poolContext.schedulingContext, nil
}

// populateNodeDb adds all the nodes and jobs associated with a particular pool to the nodeDb.
func (l *FairSchedulingAlgo) populateNodeDb(nodeDb *nodedb.NodeDb, nodeFactory *internaltypes.NodeFactory, jobs []*jobdb.Job, nodes []*schedulerobjects.Node) error {
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
		dbNode, err := nodeFactory.FromSchedulerObjectsNode(node)
		if err != nil {
			return err
		}

		if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobsByNodeId[node.Id], dbNode); err != nil {
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
