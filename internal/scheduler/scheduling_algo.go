package scheduler

import (
	"github.com/armadaproject/armada/pkg/api"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"golang.org/x/exp/maps"
	"golang.org/x/time/rate"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/stringinterner"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/fairness"
	"github.com/armadaproject/armada/internal/scheduler/floatingresources"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/queue"
	"github.com/armadaproject/armada/internal/scheduler/reports"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
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
	limiterByQueue        map[string]*rate.Limiter
	clock                 clock.Clock
	stringInterner        *stringinterner.StringInterner
	resourceListFactory   *internaltypes.ResourceListFactory
	floatingResourceTypes *floatingresources.FloatingResourceTypes
}

func NewFairSchedulingAlgo(
	config configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	queueCache queue.QueueCache,
	schedulingContextRepository *reports.SchedulingContextRepository,
	stringInterner *stringinterner.StringInterner,
	resourceListFactory *internaltypes.ResourceListFactory,
	floatingResourceTypes *floatingresources.FloatingResourceTypes,
) (*FairSchedulingAlgo, error) {
	return &FairSchedulingAlgo{
		schedulingConfig:            config,
		executorRepository:          executorRepository,
		queueCache:                  queueCache,
		schedulingContextRepository: schedulingContextRepository,
		limiter:                     rate.NewLimiter(rate.Limit(config.MaximumSchedulingRate), config.MaximumSchedulingBurst),
		limiterByQueue:              make(map[string]*rate.Limiter),
		clock:                       clock.RealClock{},
		stringInterner:              stringInterner,
		resourceListFactory:         resourceListFactory,
		floatingResourceTypes:       floatingResourceTypes,
	}, nil
}

// Schedule assigns jobs to nodes in the same way as the old lease call.
// It iterates over each executor in turn (using lexicographical order) and assigns the jobs using a LegacyScheduler, before moving onto the next executor.
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *FairSchedulingAlgo) Schedule(
	ctx *armadacontext.Context,
	txn *jobdb.Txn,
) (*SchedulerResult, error) {
	overallSchedulerResult := &SchedulerResult{
		NodeIdByJobId:                make(map[string]string),
		AdditionalAnnotationsByJobId: make(map[string]map[string]string),
	}

	executors, err := l.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}
	poolsToSchedule := []string{}
	ctx.Infof("Looping over pools %s", strings.Join(poolsToSchedule, " "))
	for _, pool := range poolsToSchedule {
		start := time.Now()
		schedulerResult, sctx, err := l.schedulePool(ctx, pool, executors, txn)
		if err != nil {
			return nil, errors.Wrap(err, "error scheduling on pool "+pool)
		}
		ctx.Infof("Scheduled on executor pool %s in %v", pool, time.Since(start))

		l.schedulingContextRepository.StoreSchedulingContext(sctx)
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
		maps.Copy(overallSchedulerResult.AdditionalAnnotationsByJobId, schedulerResult.AdditionalAnnotationsByJobId)
	}
	return overallSchedulerResult, nil
}

type fairSchedulingAlgoContext struct {
	activeQueues     []*api.Queue
	demandByQueue    map[string]schedulerobjects.QuantityByTAndResourceType[string]
	allocatedByQueue map[string]schedulerobjects.QuantityByTAndResourceType[string]
	nodeIdByJobId    map[string]string
	jobIdsByGangId   map[string]map[string]bool
	gangIdByJobId    map[string]string
}

func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(pool string, txn *jobdb.Txn) (*fairSchedulingAlgoContext, error) {

	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	demandByQueue := make(map[string]schedulerobjects.QuantityByTAndResourceType[string])
	allocatedByQueue := make(map[string]schedulerobjects.QuantityByTAndResourceType[string])

	for _, job := range txn.GetAll() {

		// We don't care about terminal jobs or jobs that don't belong to this pool
		if job.InTerminalState() || !job.InPool(pool) {
			continue
		}

		queueResources, ok := demandByQueue[job.Queue()]
		if !ok {
			queueResources = schedulerobjects.QuantityByTAndResourceType[string]{}
			demandByQueue[job.Queue()] = queueResources
		}
		pcResources, ok := queueResources[job.PriorityClassName()]
		if !ok {
			pcResources = schedulerobjects.NewResourceList(len(job.PodRequirements().ResourceRequirements.Requests))
			queueResources[job.PriorityClassName()] = pcResources
		}
		pcResources.AddV1ResourceList(job.PodRequirements().ResourceRequirements.Requests)

		if job.Queued() || job.LatestRun == nil {
			continue
		}
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

	return &fairSchedulingAlgoContext{
		demandByQueue:    demandByQueue,
		allocatedByQueue: allocatedByQueue,
		jobIdsByGangId:   jobIdsByGangId,
		gangIdByJobId:    gangIdByJobId,
	}, nil
}

// schedulePool schedules jobs on nodes that belong to a given pool.
func (l *FairSchedulingAlgo) schedulePool(ctx *armadacontext.Context, pool string, executors []*schedulerobjects.Executor, txn *jobdb.Txn) (*SchedulerResult, *schedulercontext.SchedulingContext, error) {

	fsctx, err := l.newFairSchedulingAlgoContext(pool, txn)
	if err != nil {
		return nil, nil, err
	}

	nodeDb, err := l.createNodeDb(executors, pool, txn)
	if err != nil {
		return nil, nil, err
	}

	// Right now we only support DominantResourceFairness.
	// If we want to support other fairness models it would need to be done here
	fairnessCostProvider, err := fairness.NewDominantResourceFairness(nodeDb.TotalResources(), l.schedulingConfig)
	if err != nil {
		return nil, nil, err
	}
	sctx := schedulercontext.NewSchedulingContext(
		pool,
		fairnessCostProvider,
		l.limiter,
		nodeDb.TotalResources(),
	)

	constraints := schedulerconstraints.NewSchedulingConstraints(
		pool,
		nodeDb.TotalResources(),
		l.schedulingConfig,
		fsctx.activeQueues,
	)

	for _, queue := range fsctx.activeQueues {
		demand := fsctx.demandByQueue[queue.Name]
		cappedDemand := constraints.CapResources(queue.Name, demand)
		var weight float64 = 0
		if queue.PriorityFactor > 0 {
			weight = 1 / queue.PriorityFactor
		}
		queueLimiter := l.getOrCreateQueueLimiter(queue)
		queueLimiter.SetLimi                                                                                                               tAt(l.clock.Now(), rate.Limit(l.schedulingConfig.MaximumPerQueueSchedulingRate))
		if err := sctx.AddQueueSchedulingContext(queue.Name, weight, fsctx.allocatedByQueue[queue.Name], demand.AggregateByResource(), cappedDemand.AggregateByResource(), queueLimiter); err != nil {
			return nil, nil, err
		}
	}

	sctx.UpdateFairShares()
	scheduler := NewPreemptingQueueScheduler(
		sctx,
		constraints,
		l.floatingResourceTypes,
		l.schedulingConfig.ProtectedFractionOfFairShare,
		NewSchedulerJobRepositoryAdapter(txn),
		nodeDb,
		fsctx.nodeIdByJobId,
		fsctx.jobIdsByGangId,
		fsctx.gangIdByJobId,
	)
	if l.schedulingConfig.AlwaysAttemptScheduling {
		scheduler.SkipUnsuccessfulSchedulingKeyCheck()
	}
	if l.schedulingConfig.EnableAssertions {
		scheduler.EnableAssertions()
	}

	result, err := scheduler.Schedule(ctx)
	if err != nil {
		return nil, nil, err
	}
	for i, jctx := range result.PreemptedJobs {
		jobDbJob := jctx.Job
		if run := jobDbJob.LatestRun(); run != nil {
			jobDbJob = jobDbJob.WithUpdatedRun(run.WithFailed(true))
		} else {
			return nil, nil, errors.Errorf("attempting to preempt job %s with no associated runs", jobDbJob.Id())
		}
		result.PreemptedJobs[i].Job = jobDbJob.WithQueued(false).WithFailed(true)
	}
	for i, jctx := range result.ScheduledJobs {
		jobDbJob := jctx.Job
		jobId := jobDbJob.Id()
		nodeId := result.NodeIdByJobId[jobId]
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

// SchedulerJobRepositoryAdapter allows jobDb implement the JobRepository interface.
// TODO: Pass JobDb into the scheduler instead of using this shim to convert to a JobRepo.
type SchedulerJobRepositoryAdapter struct {
	txn *jobdb.Txn
}

func NewSchedulerJobRepositoryAdapter(txn *jobdb.Txn) *SchedulerJobRepositoryAdapter {
	return &SchedulerJobRepositoryAdapter{
		txn: txn,
	}
}

// GetQueueJobIds is necessary to implement the JobRepository interface, which we need while transitioning from the old
// to new scheduler.
func (repo *SchedulerJobRepositoryAdapter) GetQueueJobIds(queue string) []string {
	rv := make([]string, 0)
	it := repo.txn.QueuedJobs(queue)
	for v, _ := it.Next(); v != nil; v, _ = it.Next() {
		rv = append(rv, v.Id())
	}
	return rv
}

// GetExistingJobsByIds is necessary to implement the JobRepository interface which we need while transitioning from the
// old to new scheduler.
func (repo *SchedulerJobRepositoryAdapter) GetExistingJobsByIds(ids []string) []*jobdb.Job {
	rv := make([]*jobdb.Job, 0, len(ids))
	for _, id := range ids {
		if job := repo.txn.GetById(id); job != nil {
			rv = append(rv, job)
		}
	}
	return rv
}

// createNodeDb  creates a new node db populated with jobs and nodes.
func (l *FairSchedulingAlgo) createNodeDb(executors []*schedulerobjects.Executor, pool string, jobdbTxn *jobdb.Txn) (*nodedb.NodeDb, error) {
	nodeDb, err := nodedb.NewNodeDb(
		l.schedulingConfig.PriorityClasses,
		l.schedulingConfig.IndexedResources,
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
		l.schedulingConfig.WellKnownNodeTypes,
		l.stringInterner,
		l.resourceListFactory,
	)
	if err != nil {
		return nil, err
	}
	txn := nodeDb.Txn(true)
	defer txn.Abort()
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			nodePool := GetNodePool(node, executor)
			if nodePool == pool {
				jobs := make([]*jobdb.Job, 0)
				for runId, _ := range node.StateByJobRunId {
					j := jobdbTxn.GetByRunId(uuid.MustParse(runId))
					if j != nil {
						jobs = append(jobs, j)
					}
				}
				if err := nodeDb.CreateAndInsertWithJobDbJobsWithTxn(txn, jobs, node); err != nil {
					return nil, err
				}
			}
		}
	}
	txn.Commit()
	return nodeDb, nil
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
		executorRuns, err := executor.AllRuns()
		if err != nil {
			logging.
				WithStacktrace(ctx, err).
				Errorf("failed to retrieve runs for executor %s; will not be considered for scheduling", executor.Id)
			continue
		}
		executorRunIds := make(map[uuid.UUID]bool, len(executorRuns))
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

func (l *FairSchedulingAlgo) getOrCreateQueueLimiter(queue *api.Queue) *rate.Limiter {
	queueLimiter, ok := l.limiterByQueue[queue.Name]
	if !ok {
		queueLimiter = rate.NewLimiter(
			rate.Limit(l.schedulingConfig.MaximumPerQueueSchedulingRate),
			l.schedulingConfig.MaximumPerQueueSchedulingBurst,
		)
		l.limiterByQueue[queue.Name] = queueLimiter
	}
	return queueLimiter
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
