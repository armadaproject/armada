package scheduler

import (
	"context"
	armadaslices "github.com/armadaproject/armada/internal/common/slices"
	"math/rand"
	"strings"
	"time"

	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	schedulerconstraints "github.com/armadaproject/armada/internal/scheduler/constraints"
	schedulercontext "github.com/armadaproject/armada/internal/scheduler/context"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/interfaces"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingAlgo is the interface between the Pulsar-backed scheduler and the
// algorithm deciding which jobs to schedule and preempt.
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes.
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided.
	Schedule(ctx context.Context, txn *jobdb.Txn, jobDb *jobdb.JobDb) (*SchedulerResult, error)
}

// FairSchedulingAlgo is a SchedulingAlgo based on PreemptingQueueScheduler.
type FairSchedulingAlgo struct {
	schedulingConfig            configuration.SchedulingConfig
	executorRepository          database.ExecutorRepository
	queueRepository             database.QueueRepository
	schedulingContextRepository *SchedulingContextRepository
	rand                        *rand.Rand // injected here for repeatable testing
	previousScheduleClusterId   string
	maxSchedulingDuration       time.Duration
	clock                       clock.Clock
	// Function that is called every time a executor is scheduled. Useful for testing.
	onExecutorScheduled func(executor *schedulerobjects.Executor)
}

func NewFairSchedulingAlgo(
	config configuration.SchedulingConfig,
	maxSchedulingDuration time.Duration,
	executorRepository database.ExecutorRepository,
	queueRepository database.QueueRepository,
	schedulingContextRepository *SchedulingContextRepository,
) (*FairSchedulingAlgo, error) {
	if _, ok := config.Preemption.PriorityClasses[config.Preemption.DefaultPriorityClass]; !ok {
		return nil, errors.Errorf("default priority class %s is missing from priority class mapping %v", config.Preemption.DefaultPriorityClass, config.Preemption.PriorityClasses)
	}
	return &FairSchedulingAlgo{
		schedulingConfig:            config,
		executorRepository:          executorRepository,
		queueRepository:             queueRepository,
		schedulingContextRepository: schedulingContextRepository,
		maxSchedulingDuration:       maxSchedulingDuration,
		rand:                        util.NewThreadsafeRand(time.Now().UnixNano()),
		clock:                       clock.RealClock{},
		onExecutorScheduled:         func(executor *schedulerobjects.Executor) {},
	}, nil
}

// Schedule assigns jobs to nodes in the same way as the old lease call.
// It iterates over each executor in turn (using lexicographical order) and assigns the jobs using a LegacyScheduler, before moving onto the next executor.
// It maintains state of which executors it has considered already and may take multiple Schedule() calls to consider all executors if scheduling is slow.
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *FairSchedulingAlgo) Schedule(
	ctx context.Context,
	txn *jobdb.Txn,
	jobDb *jobdb.JobDb,
) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	fsctx, err := l.newFairSchedulingAlgoContext(ctx, txn, jobDb)
	if err != nil {
		return nil, err
	}
	overallSchedulerResult := &SchedulerResult{
		NodeIdByJobId: make(map[string]string),
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, l.maxSchedulingDuration)
	defer cancel()

	allExecutorsConsidered := false
	executorsToSchedule := fsctx.getExecutorsToSchedule(l.previousScheduleClusterId)
	for i, executor := range executorsToSchedule {
		if ctxWithTimeout.Err() != nil {
			// We've reached the scheduling time limit, exit gracefully
			log.Infof("ending scheduling round early as we have hit the maximum scheduling duration")
			break
		}

		log.Infof("scheduling on %s", executor.Id)
		schedulerResult, sctx, err := l.scheduleOnExecutors(
			ctxWithTimeout,
			fsctx,
			executor.Pool,
			executor.MinimumJobSize,
			[]*schedulerobjects.Executor{executor},
		)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Infof("stopped scheduling on %s early as we have hit the maximum scheduling duration", executor.Id)
				// We've reached the scheduling time limit, exit gracefully
				break
			}
			return nil, err
		}
		if l.schedulingContextRepository != nil {
			if err := l.schedulingContextRepository.AddSchedulingContext(sctx); err != nil {
				logging.WithStacktrace(log, err).Error("failed to add scheduling context")
			}
		}

		// Update jobDb.
		preemptedJobs := PreemptedJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
		scheduledJobs := ScheduledJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
		if err := jobDb.Upsert(txn, preemptedJobs); err != nil {
			// TODO: We need to do something here to mark the jobs as preempted in the jobDb.
			return nil, err
		}
		if err := jobDb.Upsert(txn, scheduledJobs); err != nil {
			return nil, err
		}

		// Aggregate changes across executors.
		overallSchedulerResult.PreemptedJobs = append(overallSchedulerResult.PreemptedJobs, schedulerResult.PreemptedJobs...)
		overallSchedulerResult.ScheduledJobs = append(overallSchedulerResult.ScheduledJobs, schedulerResult.ScheduledJobs...)
		maps.Copy(overallSchedulerResult.NodeIdByJobId, schedulerResult.NodeIdByJobId)

		// Update fsctx.
		fsctx.allocationByPoolAndQueueAndPriorityClass[executor.Pool] = sctx.AllocatedByQueueAndPriority()

		// Update result to mark this executor as scheduled
		l.previousScheduleClusterId = executor.Id
		l.onExecutorScheduled(executor)

		if i+1 == len(executorsToSchedule) {
			allExecutorsConsidered = true
		}
	}
	if allExecutorsConsidered {
		log.Infof("successfully scheduled on all executors")
	}
	return overallSchedulerResult, nil
}

type JobQueueIteratorAdapter struct {
	it *immutable.SortedSetIterator[*jobdb.Job]
}

func (it *JobQueueIteratorAdapter) Next() (interfaces.LegacySchedulerJob, error) {
	if it.it.Done() {
		return nil, nil
	}
	j, _ := it.it.Next()
	return j, nil
}

type fairSchedulingAlgoContext struct {
	priorityFactorByQueue                    map[string]float64
	isActiveByQueueName                      map[string]bool
	totalCapacity                            schedulerobjects.ResourceList
	jobsByExecutorId                         map[string][]*jobdb.Job
	nodeIdByJobId                            map[string]string
	jobIdsByGangId                           map[string]map[string]bool
	gangIdByJobId                            map[string]string
	allocationByPoolAndQueueAndPriorityClass map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string]
	executors                                []*schedulerobjects.Executor
	txn                                      *jobdb.Txn
	jobDb                                    *jobdb.JobDb
}

// This function will return executors in the order they should be scheduled in
// The order is lexicographical on executor id and adjusted to start with the id next after previousScheduledExecutorId
// Example executors ids C, A, D, B
// If previousScheduledExecutorId is blank
//   - return executors in order A, B, C, D
//
// If previousScheduledExecutorId is B
//   - return executors in order C, D, A, B
func (f fairSchedulingAlgoContext) getExecutorsToSchedule(previousScheduledExecutorId string) []*schedulerobjects.Executor {
	sortedExecutors := f.executors
	slices.SortStableFunc(sortedExecutors, func(a, b *schedulerobjects.Executor) bool {
		return strings.Compare(a.Id, b.Id) < 1
	})

	executorsToSchedule := make([]*schedulerobjects.Executor, 0, len(sortedExecutors))
	if previousScheduledExecutorId == "" {
		executorsToSchedule = sortedExecutors
	} else {
		for i, executor := range sortedExecutors {
			if executor.Id > previousScheduledExecutorId {
				executorsToSchedule = append(executorsToSchedule, sortedExecutors[i:]...)
				executorsToSchedule = append(executorsToSchedule, sortedExecutors[:i]...)
				break
			} else if i+1 == len(sortedExecutors) {
				// This means all executors ids are less than previousScheduleClusterId
				executorsToSchedule = sortedExecutors
			}
		}
	}
	return executorsToSchedule
}

func (l *FairSchedulingAlgo) newFairSchedulingAlgoContext(ctx context.Context, txn *jobdb.Txn, jobDb *jobdb.JobDb) (*fairSchedulingAlgoContext, error) {
	executors, err := l.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}
	executors = l.filterStaleExecutors(executors)

	queues, err := l.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}
	priorityFactorByQueue := make(map[string]float64)
	for _, queue := range queues {
		priorityFactorByQueue[queue.Name] = queue.Weight
	}

	// Get the total capacity available across executors.
	totalCapacity := schedulerobjects.ResourceList{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			totalCapacity.Add(node.TotalResources)
		}
	}

	// Create a map of jobs associated with each executor.
	isActiveByQueueName := make(map[string]bool, len(queues))
	jobsByExecutorId := make(map[string][]*jobdb.Job)
	nodeIdByJobId := make(map[string]string)
	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	for _, job := range jobDb.GetAll(txn) {
		isActiveByQueueName[job.Queue()] = true
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
		nodeId := run.Node()
		if nodeId == "" {
			return nil, errors.Errorf("run %s of job %s is not queued but is not assigned to a node", run.Id(), job.Id())
		}
		jobsByExecutorId[executorId] = append(jobsByExecutorId[executorId], job)
		nodeIdByJobId[job.Id()] = nodeId
		gangId, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(job)
		if err != nil {
			return nil, err
		}
		if isGangJob {
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
	totalAllocationByPoolAndQueue := l.aggregateAllocationByPoolAndQueueAndPriorityClass(executors, jobsByExecutorId)

	// Filter out any executor that isn't acknowledging jobs in a timely fashion
	// Note that we do this after aggregating allocation across clusters for fair share.
	executors = l.filterLaggingExecutors(executors, jobsByExecutorId)

	return &fairSchedulingAlgoContext{
		priorityFactorByQueue:                    priorityFactorByQueue,
		isActiveByQueueName:                      isActiveByQueueName,
		totalCapacity:                            totalCapacity,
		jobsByExecutorId:                         jobsByExecutorId,
		nodeIdByJobId:                            nodeIdByJobId,
		jobIdsByGangId:                           jobIdsByGangId,
		gangIdByJobId:                            gangIdByJobId,
		allocationByPoolAndQueueAndPriorityClass: totalAllocationByPoolAndQueue,
		executors:                                executors,
		jobDb:                                    jobDb,
		txn:                                      txn,
	}, nil
}

// scheduleOnExecutors schedules jobs on a specified set of executors.
func (l *FairSchedulingAlgo) scheduleOnExecutors(
	ctx context.Context,
	fsctx *fairSchedulingAlgoContext,
	pool string,
	minimumJobSize schedulerobjects.ResourceList,
	executors []*schedulerobjects.Executor,
) (*SchedulerResult, *schedulercontext.SchedulingContext, error) {
	nodeDb, err := nodedb.NewNodeDb(
		l.schedulingConfig.Preemption.PriorityClasses,
		l.schedulingConfig.MaxExtraNodesToConsider,
		l.schedulingConfig.IndexedResources,
		l.schedulingConfig.IndexedTaints,
		l.schedulingConfig.IndexedNodeLabels,
	)
	if err != nil {
		return nil, nil, err
	}
	for _, executor := range executors {
		if err := l.addExecutorToNodeDb(nodeDb, fsctx.jobsByExecutorId[executor.Id], executor.Nodes); err != nil {
			return nil, nil, err
		}
	}

	// If there are multiple executors, use pool name instead of executorId.
	// ExecutorId is only used for reporting so this results in an aggregated report for the pool.
	executorId := pool
	if len(executors) == 1 {
		executorId = executors[0].Id
	}
	sctx := schedulercontext.NewSchedulingContext(
		executorId,
		pool,
		l.schedulingConfig.Preemption.PriorityClasses,
		l.schedulingConfig.Preemption.DefaultPriorityClass,
		l.schedulingConfig.ResourceScarcity,
		fsctx.totalCapacity,
	)
	if l.schedulingConfig.FairnessModel == configuration.DominantResourceFairness {
		sctx.EnableDominantResourceFairness(l.schedulingConfig.DominantResourceFairnessResourcesToConsider)
	}
	for queue, priorityFactor := range fsctx.priorityFactorByQueue {
		if !fsctx.isActiveByQueueName[queue] {
			// To ensure fair share is computed only from active queues, i.e., queues with jobs queued or running.
			continue
		}
		var allocatedByPriorityClass schedulerobjects.QuantityByTAndResourceType[string]
		if allocatedByQueueAndPriorityClass := fsctx.allocationByPoolAndQueueAndPriorityClass[pool]; allocatedByQueueAndPriorityClass != nil {
			allocatedByPriorityClass = allocatedByQueueAndPriorityClass[queue]
		}
		var weight float64 = 1
		if priorityFactor > 0 {
			weight = 1 / priorityFactor
		}
		if err := sctx.AddQueueSchedulingContext(queue, weight, allocatedByPriorityClass); err != nil {
			return nil, nil, err
		}
	}
	constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
		pool,
		fsctx.totalCapacity, // TODO: Make sure this is for this pool.
		minimumJobSize,
		l.schedulingConfig,
	)
	scheduler := NewPreemptingQueueScheduler(
		sctx,
		constraints,
		l.schedulingConfig.Preemption.NodeEvictionProbability,
		l.schedulingConfig.Preemption.NodeOversubscriptionEvictionProbability,
		l.schedulingConfig.Preemption.ProtectedFractionOfFairShare,
		&schedulerJobRepositoryAdapter{
			txn: fsctx.txn,
			db:  fsctx.jobDb,
		},
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

	for i, job := range result.PreemptedJobs {
		jobDbJob := job.(*jobdb.Job)
		if run := jobDbJob.LatestRun(); run != nil {
			jobDbJob = jobDbJob.WithUpdatedRun(run.WithFailed(true))
		} else {
			return nil, nil, errors.Errorf("attempting to preempt job %s with no associated runs", jobDbJob.Id())
		}
		result.PreemptedJobs[i] = jobDbJob.WithQueued(false).WithFailed(true)
	}
	for i, job := range result.ScheduledJobs {
		jobDbJob := job.(*jobdb.Job)
		nodeId := result.NodeIdByJobId[jobDbJob.GetId()]
		if nodeId == "" {
			return nil, nil, errors.Errorf("job %s not mapped to any node", jobDbJob.GetId())
		}
		if node, err := nodeDb.GetNode(nodeId); err != nil {
			return nil, nil, err
		} else {
			result.ScheduledJobs[i] = jobDbJob.WithQueued(false).WithNewRun(node.Executor, node.Id)
		}
	}
	return result, sctx, nil
}

// Adapter to make jobDb implement the JobRepository interface.
type schedulerJobRepositoryAdapter struct {
	db  *jobdb.JobDb
	txn *jobdb.Txn
}

// GetQueueJobIds is Necessary to implement the JobRepository interface, which we need while transitioning from the old
// to new scheduler.
func (repo *schedulerJobRepositoryAdapter) GetQueueJobIds(queue string) ([]string, error) {
	rv := make([]string, 0)
	it := repo.db.QueuedJobs(repo.txn, queue)
	for v, _ := it.Next(); v != nil; v, _ = it.Next() {
		rv = append(rv, v.Id())
	}
	return rv, nil
}

// GetExistingJobsByIds is necessary to implement the JobRepository interface which we need while transitioning from the
// old to new scheduler.
func (repo *schedulerJobRepositoryAdapter) GetExistingJobsByIds(ids []string) ([]interfaces.LegacySchedulerJob, error) {
	rv := make([]interfaces.LegacySchedulerJob, 0, len(ids))
	for _, id := range ids {
		if job := repo.db.GetById(repo.txn, id); job != nil {
			rv = append(rv, job)
		}
	}
	return rv, nil
}

// addExecutorToNodeDb adds all the nodes and jobs associated with a particular executor to the nodeDb.
func (l *FairSchedulingAlgo) addExecutorToNodeDb(nodeDb *nodedb.NodeDb, jobs []*jobdb.Job, nodes []*schedulerobjects.Node) error {
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
		nodeId := job.LatestRun().Node()
		if _, ok := nodesById[nodeId]; !ok {
			log.Errorf(
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
func (l *FairSchedulingAlgo) filterStaleExecutors(executors []*schedulerobjects.Executor) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	cutoff := l.clock.Now().Add(-l.schedulingConfig.ExecutorTimeout)
	for _, executor := range executors {
		if executor.LastUpdateTime.After(cutoff) {
			activeExecutors = append(activeExecutors, executor)
		} else {
			log.Debugf("Ignoring executor %s because it hasn't heartbeated since %s", executor.Id, executor.LastUpdateTime)
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
	executors []*schedulerobjects.Executor,
	leasedJobsByExecutor map[string][]*jobdb.Job,
) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	for _, executor := range executors {
		leasedJobs := leasedJobsByExecutor[executor.Id]
		executorRuns, err := executor.AllRuns()
		if err != nil {
			log.Errorf("failed to retrieve runs for executor %s; will not be considered for scheduling: %s", executor.Id, err.Error())
			continue
		}
		executorRunIds := make(map[uuid.UUID]bool, len(executorRuns))
		for _, run := range executorRuns {
			executorRunIds[run] = true
		}

		numUnacknowledgedJobs := uint(0)
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
			log.Warnf(
				"%d unacknowledged jobs on executor %s exceeds limit of %d; will not be considered for scheduling",
				numUnacknowledgedJobs, executor.Id, l.schedulingConfig.MaxUnacknowledgedJobsPerExecutor,
			)
		}

	}
	return activeExecutors
}

func (l *FairSchedulingAlgo) aggregateAllocationByPoolAndQueueAndPriorityClass(executors []*schedulerobjects.Executor, jobsByExecutorId map[string][]*jobdb.Job) map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string] {
	rv := make(map[string]map[string]schedulerobjects.QuantityByTAndResourceType[string])
	for _, executor := range executors {
		allocationByQueue := rv[executor.Pool]
		if allocationByQueue == nil {
			allocationByQueue = make(map[string]schedulerobjects.QuantityByTAndResourceType[string])
			rv[executor.Pool] = allocationByQueue
		}
		for _, job := range jobsByExecutorId[executor.Id] {
			queue := job.Queue()
			allocation := allocationByQueue[queue]
			if allocation == nil {
				allocation = make(schedulerobjects.QuantityByTAndResourceType[string])
				allocationByQueue[queue] = allocation
			}
			allocation.AddV1ResourceList(job.GetPriorityClassName(), job.GetResourceRequirements().Requests)
		}
	}
	return rv
}
