package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/benbjohnson/immutable"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/exp/maps"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingAlgo is the interface between the Pulsar-backed scheduler and the
// algorithm deciding which jobs to schedule and preempt.
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided
	// It should return a slice containing all scheduled jobs.
	Schedule(ctx context.Context, txn *jobdb.Txn, jobDb *jobdb.JobDb) (*SchedulerResult, error)
}

// LegacySchedulingAlgo is a SchedulingAlgo that schedules jobs in the same way as the old lease call
type LegacySchedulingAlgo struct {
	config                  configuration.SchedulingConfig
	executorRepository      database.ExecutorRepository
	queueRepository         database.QueueRepository
	priorityClassPriorities []int32
	indexedResources        []string
	rand                    *rand.Rand // injected here for repeatable testing
	clock                   clock.Clock
}

func NewLegacySchedulingAlgo(
	config configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	queueRepository database.QueueRepository,
) *LegacySchedulingAlgo {
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

	return &LegacySchedulingAlgo{
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
func (l *LegacySchedulingAlgo) Schedule(
	ctx context.Context,
	txn *jobdb.Txn,
	jobDb *jobdb.JobDb,
) (*SchedulerResult, error) {
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

	// Get the total capacity available across all executors.
	totalCapacity := schedulerobjects.ResourceList{}
	for _, executor := range executors {
		for _, node := range executor.Nodes {
			totalCapacity.Add(node.TotalResources)
		}
	}

	// Create a map of jobs associated with each executor.
	jobsByExecutor := make(map[string][]*jobdb.Job)
	for _, job := range jobDb.GetAll(txn) {
		if !job.Queued() && job.HasRuns() {
			executor := job.LatestRun().Executor()
			jobsByExecutor[executor] = append(jobsByExecutor[executor], job)
		}
	}

	// Used to calculate fair share.
	resourceUsagebyPool, err := aggregateUsage(executors, txn, jobDb)
	if err != nil {
		return nil, err
	}

	overallSchedulerResult := &SchedulerResult{
		NodeIdByJobId: make(map[string]string),
	}
	for _, executor := range executors {
		log.Infof("attempting to schedule jobs on %s", executor.Id)
		totalResourceUsageByQueue := resourceUsagebyPool[executor.Pool]
		schedulerResult, err := l.scheduleOnExecutor(
			ctx,
			executor,
			jobsByExecutor[executor.Id],
			totalResourceUsageByQueue,
			totalCapacity,
			priorityFactorByQueue,
			jobDb,
			txn,
		)
		if err != nil {
			return nil, err
		}
		preemptedJobs := PreemptedJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
		scheduledJobs := ScheduledJobsFromSchedulerResult[*jobdb.Job](schedulerResult)
		if err := jobDb.Upsert(txn, preemptedJobs); err != nil {
			return nil, err
		}
		if err := jobDb.Upsert(txn, scheduledJobs); err != nil {
			return nil, err
		}
		overallSchedulerResult.PreemptedJobs = append(overallSchedulerResult.PreemptedJobs, schedulerResult.PreemptedJobs...)
		overallSchedulerResult.ScheduledJobs = append(overallSchedulerResult.ScheduledJobs, schedulerResult.ScheduledJobs...)
		maps.Copy(overallSchedulerResult.NodeIdByJobId, schedulerResult.NodeIdByJobId)

		resourceUsagebyPool[executor.Pool] = schedulerResult.AllocatedByQueueAndPriority
	}

	return overallSchedulerResult, nil
}

type JobQueueIteratorAdapter struct {
	it *immutable.SortedSetIterator[*jobdb.Job]
}

func (it *JobQueueIteratorAdapter) Next() (LegacySchedulerJob, error) {
	if it.it.Done() {
		return nil, nil
	}
	j, _ := it.it.Next()
	return j, nil
}

// scheduleOnExecutor schedules jobs on a single executor
func (l *LegacySchedulingAlgo) scheduleOnExecutor(
	ctx context.Context,
	executor *schedulerobjects.Executor,
	leasedJobs []*jobdb.Job,
	allocatedByQueueAndPriority map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	totalCapacity schedulerobjects.ResourceList,
	priorityFactorByQueue map[string]float64,
	db *jobdb.JobDb,
	txn *jobdb.Txn,
) (*SchedulerResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nodeDb, err := l.constructNodeDb(executor.Nodes, leasedJobs, l.config.Preemption.PriorityClasses)
	if err != nil {
		return nil, err
	}
	constraints := SchedulingConstraintsFromSchedulingConfig(
		executor.Id,
		executor.Pool,
		executor.MinimumJobSize,
		l.config,
		totalCapacity,
	)
	scheduler := NewRescheduler(
		*constraints,
		l.config,
		&schedulerJobRepositoryAdapter{
			txn: txn,
			db:  db,
		},
		nodeDb,
		priorityFactorByQueue,
		allocatedByQueueAndPriority,
		// TODO: Add missing maps to enable gang preemption.
		nil,
		nil,
		nil,
	)
	if l.config.EnableAssertions {
		scheduler.EnableAssertions()
	}
	result, err := scheduler.Schedule(ctx)
	if err != nil {
		return nil, err
	}
	// TODO: Add a repo to enable querying for scheduler reports.

	for i, job := range result.PreemptedJobs {
		jobDbJob := job.(*jobdb.Job)
		if run := jobDbJob.LatestRun(); run != nil {
			jobDbJob = jobDbJob.WithUpdatedRun(run.WithFailed(true))
		} else {
			return nil, errors.Errorf("attempting to preempt job %s with no associated runs", jobDbJob.Id())
		}
		result.ScheduledJobs[i] = jobDbJob.WithQueued(false).WithFailed(true)
	}
	for i, job := range result.ScheduledJobs {
		jobDbJob := job.(*jobdb.Job)
		nodeId := result.NodeIdByJobId[jobDbJob.GetId()]
		if nodeId == "" {
			return nil, errors.Errorf("job %s not mapped to any node", jobDbJob.GetId())
		}
		if node, err := nodeDb.GetNode(nodeId); err != nil {
			return nil, err
		} else {
			result.ScheduledJobs[i] = jobDbJob.WithQueued(false).WithNewRun(executor.Id, node.Name)
		}
	}
	return result, nil
}

// Adapter to make jobDb implement the JobRepository interface.
type schedulerJobRepositoryAdapter struct {
	db  *jobdb.JobDb
	txn *jobdb.Txn
}

// Necessary to implement the JobRepository interface,
// which we need while transitioning from the old to new scheduler.
func (repo *schedulerJobRepositoryAdapter) GetQueueJobIds(queue string) ([]string, error) {
	rv := make([]string, 0)
	it := repo.db.QueuedJobs(repo.txn, queue)
	for v, _ := it.Next(); v != nil; v, _ = it.Next() {
		rv = append(rv, v.Id())
	}
	return rv, nil
}

// Necessary to implement the JobRepository interface,
// which we need while transitioning from the old to new scheduler.
func (repo *schedulerJobRepositoryAdapter) GetExistingJobsByIds(ids []string) ([]LegacySchedulerJob, error) {
	rv := make([]LegacySchedulerJob, 0, len(ids))
	for _, id := range ids {
		if job := repo.db.GetById(repo.txn, id); job != nil {
			rv = append(rv, job)
		}
	}
	return rv, nil
}

// constructNodeDb constructs a node db with all jobs bound to it.
func (l *LegacySchedulingAlgo) constructNodeDb(nodes []*schedulerobjects.Node, jobs []*jobdb.Job, priorityClasses map[string]configuration.PriorityClass) (*NodeDb, error) {
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
		node, err := BindPodToNode(req, node)
		if err != nil {
			return nil, err
		}
		nodesByName[node.Name] = node
	}

	// Nodes to be considered by the scheduler.
	nodeDb, err := NewNodeDb(
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

func (l *LegacySchedulingAlgo) filterStaleExecutors(allExecutors []*schedulerobjects.Executor) []*schedulerobjects.Executor {
	activeExecutors := make([]*schedulerobjects.Executor, 0, len(allExecutors))
	cutoff := l.clock.Now().Add(-l.config.ExecutorTimeout)
	for _, executor := range allExecutors {
		if executor.LastUpdateTime.After(cutoff) {
			activeExecutors = append(activeExecutors, executor)
		} else {
			log.Debugf("Ignoring executor %s because it hasn't heartbeated since %s", executor.Id, executor.LastUpdateTime)
		}
	}
	return activeExecutors
}

// aggregateUsage creates a map of usage by pool.
func aggregateUsage(executors []*schedulerobjects.Executor, txn *jobdb.Txn, jobDb *jobdb.JobDb) (map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType, error) {
	usageByPool := make(map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType, 0)
	for _, executor := range executors {
		poolUsage, ok := usageByPool[executor.Pool]
		if !ok {
			poolUsage = make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
			usageByPool[executor.Pool] = poolUsage
		}
		executorUsage, err := aggregateUsageForExecutor(executor, txn, jobDb)
		if err != nil {
			return nil, err
		}
		for queue, usage := range executorUsage {
			queueUsage, ok := poolUsage[queue]
			if !ok {
				queueUsage = make(schedulerobjects.QuantityByPriorityAndResourceType)
				poolUsage[queue] = queueUsage
			}
			queueUsage.Add(usage)
		}
	}
	return usageByPool, nil
}

// aggregateUsageForExecutor aggregates the resource usage for a given executor, first by queue and then by priority class
// This is done by taking all the job runs that the executor last reported owning, looking up the corresponding job in jobdb
// and then aggregating the resources of each job
func aggregateUsageForExecutor(executor *schedulerobjects.Executor, txn *jobdb.Txn, jobDb *jobdb.JobDb) (map[string]schedulerobjects.QuantityByPriorityAndResourceType, error) {
	allRuns, err := executor.AllRuns()
	if err != nil {
		return nil, err
	}
	usageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, runId := range allRuns {
		job := jobDb.GetByRunId(txn, runId)
		if job != nil {
			queueUsage, ok := usageByQueue[job.Queue()]
			if !ok {
				queueUsage = make(schedulerobjects.QuantityByPriorityAndResourceType)
				usageByQueue[job.Queue()] = queueUsage
			}
			jobQty, err := schedulerQuantityFromJob(job)
			if err != nil {
				return nil, err
			}
			queueUsage.Add(jobQty)
		} else {
			// if the job isn't in jobdb then it is no longer active, and we can ignore it
			log.Debugf("Ignoring run %s", runId)
		}
	}
	return usageByQueue, nil
}

func schedulerQuantityFromJob(job *jobdb.Job) (schedulerobjects.QuantityByPriorityAndResourceType, error) {
	objectRequirements := job.JobSchedulingInfo().GetObjectRequirements()
	if len(objectRequirements) == 0 {
		return nil, errors.New(fmt.Sprintf("no objectRequirements attached to job %s", job.Id()))
	}
	requirements := objectRequirements[0].GetPodRequirements()
	jobRequests := requirements.ResourceRequirements.Requests
	resources := make(map[string]resource.Quantity, 0)
	for k, v := range jobRequests {
		resources[string(k)] = v
	}
	return schedulerobjects.QuantityByPriorityAndResourceType{
		requirements.Priority: schedulerobjects.ResourceList{Resources: resources},
	}, nil
}
