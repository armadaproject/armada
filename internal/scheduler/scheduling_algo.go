package scheduler

import (
	"context"
	"math/rand"
	"time"

	"github.com/benbjohnson/immutable"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
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
	config                      configuration.SchedulingConfig
	executorRepository          database.ExecutorRepository
	queueRepository             database.QueueRepository
	schedulingContextRepository *SchedulingContextRepository // TODO: Initialise.
	priorityClassPriorities     []int32
	indexedResources            []string
	rand                        *rand.Rand // injected here for repeatable testing
	clock                       clock.Clock
}

func NewFairSchedulingAlgo(
	config configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	queueRepository database.QueueRepository,
) *FairSchedulingAlgo {
	priorities := make([]int32, 0)
	if len(config.Preemption.PriorityClasses) > 0 {
		for _, p := range config.Preemption.PriorityClasses {
			priorities = append(priorities, p.Priority)
		}
	} else {
		priorities = append(priorities, 0)
	}

	indexedResources := config.IndexedResources
	if len(indexedResources) == 0 {
		indexedResources = []string{"cpu", "memory"}
	}

	return &FairSchedulingAlgo{
		config:                  config,
		executorRepository:      executorRepository,
		queueRepository:         queueRepository,
		priorityClassPriorities: priorities,
		indexedResources:        indexedResources,
		rand:                    util.NewThreadsafeRand(time.Now().UnixNano()),
		clock:                   clock.RealClock{},
	}
}

// Schedule assigns jobs to nodes in the same way as the old lease call.
// It iterates over each executor in turn (using a random order) and assigns the jobs using a LegacyScheduler, before moving onto the next executor
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *FairSchedulingAlgo) Schedule(
	ctx context.Context,
	txn *jobdb.Txn,
	jobDb *jobdb.JobDb,
) (*SchedulerResult, error) {
	log := ctxlogrus.Extract(ctx)
	accounting, err := l.newFairSchedulingAlgoContext(ctx, txn, jobDb)
	if err != nil {
		return nil, err
	}
	overallSchedulerResult := &SchedulerResult{
		NodeIdByJobId: make(map[string]string),
	}
	for _, executor := range accounting.executors {
		log.Infof("scheduling on %s", executor.Id)
		schedulerResult, sctx, err := l.scheduleOnExecutor(
			ctx,
			accounting,
			txn,
			executor,
			jobDb,
		)
		if err != nil {
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

		// Update accounting.
		accounting.totalAllocationByPoolAndQueue[executor.Pool] = sctx.AllocatedByQueueAndPriority()
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
	priorityFactorByQueue         map[string]float64
	totalCapacity                 schedulerobjects.ResourceList
	jobsByExecutorId              map[string][]*jobdb.Job
	nodeIdByJobId                 map[string]string
	jobIdsByGangId                map[string]map[string]bool
	gangIdByJobId                 map[string]string
	totalAllocationByPoolAndQueue map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType
	executors                     []*schedulerobjects.Executor
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
	jobsByExecutorId := make(map[string][]*jobdb.Job)
	nodeIdByJobId := make(map[string]string)
	jobIdsByGangId := make(map[string]map[string]bool)
	gangIdByJobId := make(map[string]string)
	for _, job := range jobDb.GetAll(txn) {
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
		gangId, _, isGangJob, err := GangIdAndCardinalityFromLegacySchedulerJob(job, l.config.Preemption.PriorityClasses)
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
	totalAllocationByPoolAndQueue := l.totalAllocationByPoolAndQueue(executors, jobsByExecutorId)

	// Filter out any executor that isn't acknowledging jobs in a timely fashion
	// Note that we do this after aggregating allocation across clusters for fair share.
	executors = l.filterLaggingExecutors(executors, jobsByExecutorId)

	return &fairSchedulingAlgoContext{
		priorityFactorByQueue:         priorityFactorByQueue,
		totalCapacity:                 totalCapacity,
		jobsByExecutorId:              jobsByExecutorId,
		nodeIdByJobId:                 nodeIdByJobId,
		jobIdsByGangId:                jobIdsByGangId,
		gangIdByJobId:                 gangIdByJobId,
		totalAllocationByPoolAndQueue: totalAllocationByPoolAndQueue,
		executors:                     executors,
	}, nil
}

// scheduleOnExecutor schedules jobs on a specified executor.
func (l *FairSchedulingAlgo) scheduleOnExecutor(
	ctx context.Context,
	accounting *fairSchedulingAlgoContext,
	txn *jobdb.Txn,
	executor *schedulerobjects.Executor,
	db *jobdb.JobDb,
) (*SchedulerResult, *schedulercontext.SchedulingContext, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nodeDb, err := l.constructNodeDb(
		executor.Nodes,
		accounting.jobsByExecutorId[executor.Id],
		l.config.Preemption.PriorityClasses,
	)
	if err != nil {
		return nil, nil, err
	}
	sctx := schedulercontext.NewSchedulingContext(
		executor.Id,
		executor.Pool,
		l.config.Preemption.PriorityClasses,
		l.config.Preemption.DefaultPriorityClass,
		l.config.ResourceScarcity,
		accounting.priorityFactorByQueue,
		accounting.totalCapacity,
		accounting.totalAllocationByPoolAndQueue[executor.Pool],
	)
	constraints := schedulerconstraints.SchedulingConstraintsFromSchedulingConfig(
		executor.MinimumJobSize,
		l.config,
	)
	scheduler := NewPreemptingQueueScheduler(
		sctx,
		constraints,
		l.config.Preemption.NodeEvictionProbability,
		l.config.Preemption.NodeOversubscriptionEvictionProbability,
		&schedulerJobRepositoryAdapter{
			txn: txn,
			db:  db,
		},
		nodeDb,
		accounting.nodeIdByJobId,
		accounting.jobIdsByGangId,
		accounting.gangIdByJobId,
	)
	if l.config.EnableAssertions {
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
			result.ScheduledJobs[i] = jobDbJob.WithQueued(false).WithNewRun(executor.Id, node.Name)
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

// constructNodeDb constructs a node db with all jobs bound to it.
func (l *FairSchedulingAlgo) constructNodeDb(nodes []*schedulerobjects.Node, jobs []*jobdb.Job, priorityClasses map[string]configuration.PriorityClass) (*nodedb.NodeDb, error) {
	nodesByName := make(map[string]*schedulerobjects.Node, len(nodes))
	for _, node := range nodes {
		nodesByName[node.Name] = node
	}
	for _, job := range jobs {
		if job.InTerminalState() || !job.HasRuns() {
			continue
		}
		assignedNode := job.LatestRun().Node()
		node, ok := nodesByName[assignedNode]
		if !ok {
			log.Warnf(
				"job %s assigned to node %s on executor %s, but no such node found",
				job.Id(), assignedNode, job.LatestRun().Executor(),
			)
			continue
		}
		req := PodRequirementFromLegacySchedulerJob(job, l.config.Preemption.PriorityClasses)
		if req == nil {
			log.Errorf("no pod spec found for job %s", job.Id())
			continue
		}
		node, err := nodedb.BindPodToNode(req, node)
		if err != nil {
			return nil, err
		}
		nodesByName[node.Name] = node
	}
	nodeDb, err := nodedb.NewNodeDb(
		priorityClasses,
		l.config.MaxExtraNodesToConsider,
		l.indexedResources,
		l.config.IndexedTaints,
		l.config.IndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	if err := nodeDb.UpsertMany(maps.Values(nodesByName)); err != nil {
		return nil, err
	}
	return nodeDb, nil
}

// filterStaleExecutors returns all executors which have sent a lease request within the duration given by l.config.ExecutorTimeout.
// This ensures that we don't continue to assign jobs to executors that are no longer active.
func (l *FairSchedulingAlgo) filterStaleExecutors(executors []*schedulerobjects.Executor) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(executors))
	cutoff := l.clock.Now().Add(-l.config.ExecutorTimeout)
	for _, executor := range executors {
		if executor.LastUpdateTime.After(cutoff) {
			activeExecutors = append(activeExecutors, executor)
		} else {
			log.Debugf("Ignoring executor %s because it hasn't heartbeated since %s", executor.Id, executor.LastUpdateTime)
		}
	}
	return activeExecutors
}

// filterLaggingExecutors returns all executors with <= l.config.MaxUnacknowledgedJobsPerExecutor unacknowledged jobs,
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
		if numUnacknowledgedJobs <= l.config.MaxUnacknowledgedJobsPerExecutor {
			activeExecutors = append(activeExecutors, executor)
		} else {
			log.Warnf(
				"%d unacknowledged jobs on executor %s exceeds limit of %d; will not be considered for scheduling",
				numUnacknowledgedJobs, executor.Id, l.config.MaxUnacknowledgedJobsPerExecutor,
			)
		}

	}
	return activeExecutors
}

func (l *FairSchedulingAlgo) totalAllocationByPoolAndQueue(executors []*schedulerobjects.Executor, jobsByExecutorId map[string][]*jobdb.Job) map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	rv := make(map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, executor := range executors {
		allocationByQueue := rv[executor.Pool]
		if allocationByQueue == nil {
			allocationByQueue = make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
			rv[executor.Pool] = allocationByQueue
		}
		for _, job := range jobsByExecutorId[executor.Id] {
			queue := job.Queue()
			allocation := allocationByQueue[queue]
			if allocation == nil {
				allocation = make(schedulerobjects.QuantityByPriorityAndResourceType)
				allocationByQueue[queue] = allocation
			}
			jobSchedulingInfo := job.JobSchedulingInfo()
			if jobSchedulingInfo != nil {
				allocation.AddResourceList(
					int32(jobSchedulingInfo.Priority),
					jobSchedulingInfo.GetTotalResourceRequest(),
				)
			}
		}
	}
	return rv
}
