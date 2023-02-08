package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/hashicorp/go-memdb"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/clock"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/database"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// SchedulingAlgo is an interface that should bne implemented by structs capable of assigning Jobs to nodes
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided
	// It should return a slice containing all scheduled jobs.
	Schedule(ctx context.Context, txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error)
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

// Schedule assigns jobs to nodes in the same way as the old lease call.  It iterates over each executor in turn
// (using a random order) and assigns the jobs using a LegacyScheduler, before moving onto the next executor
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *LegacySchedulingAlgo) Schedule(ctx context.Context, txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error) {
	executors, err := l.executorRepository.GetExecutors(ctx)
	if err != nil {
		return nil, err
	}
	executors = l.filterStaleExecutors(executors)

	queues, err := l.queueRepository.GetAllQueues()
	if err != nil {
		return nil, err
	}

	queues, err = l.filterEmptyQueues(queues, txn, jobDb)
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

	// used to calculate fair share
	resourceUsagebyPool, err := aggregateUsage(executors, txn, jobDb)
	if err != nil {
		return nil, err
	}

	jobsToSchedule := make([]*SchedulerJob, 0)
	for _, executor := range executors {
		log.Infof("Attempting to schedule jobs on %s", executor.Id)
		totalResourceUsageByQueue := resourceUsagebyPool[executor.Pool]
		jobs, err := l.scheduleOnExecutor(ctx, executor, totalResourceUsageByQueue, totalCapacity, priorityFactorByQueue, txn)
		if err != nil {
			return nil, err
		}
		if len(jobs) > 0 {
			err := jobDb.Upsert(txn, jobs)
			if err != nil {
				return nil, err
			}
			jobsToSchedule = append(jobsToSchedule, jobs...)

			// update totalResourceUsageByQueue with the jobs we just scheduled, so that fair share is
			// correct when allocating on the next cluster
			// TODO: this is far too complex- there is too much converting from one type to another
			for _, job := range jobs {
				quantityByPriorityAndResourceType, ok := totalResourceUsageByQueue[job.Queue]
				if !ok {
					quantityByPriorityAndResourceType = make(schedulerobjects.QuantityByPriorityAndResourceType)
					totalResourceUsageByQueue[job.Queue] = quantityByPriorityAndResourceType
				}
				jobQty, err := schedulerQuantityFromJob(job)
				if err != nil {
					return nil, err
				}
				quantityByPriorityAndResourceType.Add(jobQty)
			}
		}
	}

	return jobsToSchedule, nil
}

type JobQueueIteratorAdapter struct {
	it *JobQueueIterator
}

func (it *JobQueueIteratorAdapter) Next() (LegacySchedulerJob, error) {
	return it.it.NextJobItem(), nil
}

// scheduleOnExecutor schedules jobs on a single executor
func (l *LegacySchedulingAlgo) scheduleOnExecutor(
	ctx context.Context,
	executor *schedulerobjects.Executor,
	totalResourceUsageByQueue map[string]schedulerobjects.QuantityByPriorityAndResourceType,
	totalCapacity schedulerobjects.ResourceList,
	priorityFactorByQueue map[string]float64,
	txn *memdb.Txn,
) ([]*SchedulerJob, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	nodeDb, err := l.constructNodeDb(executor.Nodes, l.priorityClassPriorities)
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
	queues := make([]*Queue, 0, len(priorityFactorByQueue))
	for name, priorityFactor := range priorityFactorByQueue {
		it, err := NewJobQueueIterator(txn, name)
		if err != nil {
			return nil, err
		}
		if queue, err := NewQueue(name, priorityFactor, &JobQueueIteratorAdapter{it: it}); err != nil {
			return nil, err
		} else {
			queues = append(queues, queue)
		}
	}
	legacyScheduler, err := NewLegacyScheduler(
		ctx,
		*constraints,
		l.config,
		nodeDb,
		queues,
		totalResourceUsageByQueue)
	if err != nil {
		return nil, err
	}
	jobs, err := legacyScheduler.Schedule()
	if err != nil {
		return nil, err
	}
	updatedJobs := make([]*SchedulerJob, len(jobs))
	for i, report := range legacyScheduler.SchedulingRoundReport.SuccessfulJobSchedulingReports() {
		jobCopy := report.Job.(*SchedulerJob).DeepCopy()
		jobCopy.Queued = false
		run := JobRun{
			RunID:    uuid.New(),
			Executor: executor.Id,
		}
		jobCopy.Runs = append(jobCopy.Runs, &run)
		updatedJobs[i] = jobCopy
	}
	return updatedJobs, nil
}

func (l *LegacySchedulingAlgo) constructNodeDb(nodes []*schedulerobjects.Node, priorities []int32) (*NodeDb, error) {
	// Nodes to be considered by the scheduler.
	nodeDb, err := NewNodeDb(
		priorities,
		l.indexedResources,
		l.config.IndexedTaints,
		l.config.IndexedNodeLabels,
	)
	if err != nil {
		return nil, err
	}
	err = nodeDb.Upsert(nodes)
	if err != nil {
		return nil, err
	}
	return nodeDb, nil
}

// filterEmptyQueues returns only the queues which have leased jobs in the jobs db
func (l *LegacySchedulingAlgo) filterEmptyQueues(allQueues []*database.Queue, txn *memdb.Txn, jobDb *JobDb) ([]*database.Queue, error) {
	activeQueues := make([]*database.Queue, 0, len(allQueues))
	for _, queue := range allQueues {
		keep, err := jobDb.HasQueuedJobs(txn, queue.Name)
		if err != nil {
			return nil, err
		}
		if keep {
			activeQueues = append(activeQueues, queue)
		}
	}
	return activeQueues, nil
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

// aggregateUsage Creates a map of usage by pool
func aggregateUsage(executors []*schedulerobjects.Executor, txn *memdb.Txn, jobDb *JobDb) (map[string]map[string]schedulerobjects.QuantityByPriorityAndResourceType, error) {
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
func aggregateUsageForExecutor(executor *schedulerobjects.Executor, txn *memdb.Txn, jobDb *JobDb) (map[string]schedulerobjects.QuantityByPriorityAndResourceType, error) {
	allRuns, err := executor.AllRuns()
	if err != nil {
		return nil, err
	}
	usageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, runId := range allRuns {
		job, err := jobDb.GetByRunId(txn, runId)
		if err != nil {
			return nil, err
		}
		if job != nil {
			queueUsage, ok := usageByQueue[job.Queue]
			if !ok {
				queueUsage = make(schedulerobjects.QuantityByPriorityAndResourceType)
				usageByQueue[job.Queue] = queueUsage
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

func schedulerQuantityFromJob(job *SchedulerJob) (schedulerobjects.QuantityByPriorityAndResourceType, error) {
	objectRequirements := job.jobSchedulingInfo.GetObjectRequirements()
	if len(objectRequirements) == 0 {
		return nil, errors.New(fmt.Sprintf("no objectRequirements attached to job %s", job.GetId()))
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
