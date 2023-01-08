package scheduler

import (
	"context"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common/util"
	"github.com/G-Research/armada/internal/scheduler/database"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/hashicorp/go-memdb"
	"math/rand"
	"time"
)

// SchedulingAlgo is an interface that should bne implemented by structs capable of assigning Jobs to nodes
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided
	// It should return a slice containing all scheduled jobs.
	Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error)
}

// LegacySchedulingAlgo is a SchedulingAlgo that schedules jobs in the same way as the old lease call
type LegacySchedulingAlgo struct {
	config                  *configuration.SchedulingConfig
	executorRepository      database.ExecutorRepository
	queueRepository         database.QueueRepository
	priorityClassPriorities []int32
	indexedResources        []string
	rand                    *rand.Rand // injected here for repeatable testing
}

func NewLegacySchedulingAlgo(
	config *configuration.SchedulingConfig,
	executorRepository database.ExecutorRepository,
	queueRepository database.QueueRepository) *LegacySchedulingAlgo {
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
	}
}

// Schedule assigns jobs to nodes in the same way as the old lease call.  It iterates over each executor in turn
// (using a random order) and assigns the jobs it can using a LegacyScheduler, before moving onto the next cluster
// Newly leased jobs are updated as such in the jobDb using the transaction provided and are also returned to the caller.
func (l *LegacySchedulingAlgo) Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error) {

	clusters, err := l.executorRepository.GetExecutors()
	if err != nil {
		return nil, err
	}

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

	// Get the total capacity available across all clusters.
	totalCapacity := schedulerobjects.ResourceList{}
	for _, cluster := range clusters {
		for _, node := range cluster.Nodes {
			totalCapacity.Add(node.TotalResources)
		}
	}

	jobsToSchedule := make([]*SchedulerJob, 0)
	for _, cluster := range clusters {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		initialResourcesByQueueAndPriority := aggregateUsage(clusters, cluster.Pool)

		jobRepo := NewJobDbAdapter(txn)
		nodeDb, err := l.constructNodeDb(cluster.Nodes, l.priorityClassPriorities)
		if err != nil {
			return nil, err
		}

		constraints := SchedulingConstraintsFromSchedulingConfig(
			cluster.Name,
			cluster.Pool,
			schedulerobjects.ResourceList{Resources: cluster.MinimumJobSize},
			*l.config,
			totalCapacity,
		)

		legacyScheduler, err := NewLegacyScheduler[*SchedulerJob](
			ctx,
			*constraints,
			*l.config,
			nodeDb,
			jobRepo,
			priorityFactorByQueue,
			initialResourcesByQueueAndPriority)

		if err != nil {
			return nil, err
		}
		jobs, err := legacyScheduler.Schedule()
		cancel()
		if err != nil {
			return nil, err
		}
		updatedJobs := make([]*SchedulerJob, len(jobs))
		for _, report := range legacyScheduler.SchedulingRoundReport.SuccessfulJobSchedulingReports() {
			copy := report.Job.DeepCopy()
			copy.Queued = false
			copy.Executor = cluster.Name
			if len(report.PodSchedulingReports) > 0 {
				copy.Node = report.PodSchedulingReports[0].Node.GetId()
			}
			updatedJobs = append(updatedJobs, copy)
		}
		jobDb.Upsert(txn, updatedJobs)
		jobsToSchedule = append(jobsToSchedule, jobs...)
	}

	return jobsToSchedule, nil
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

// aggregateUsage Creates a map of usage first by executors and then by queue
// Note that the desired exector is excluded as this will be filled in later as are clusters that are not in the
// same pool as the desired cluster
func aggregateUsage(clusters []*database.Executor, pool string) map[string]schedulerobjects.QuantityByPriorityAndResourceType {
	const activeClusterExpiry = 10 * time.Minute

	// Aggregate resource usage across clusters.
	aggregatedUsageByQueue := make(map[string]schedulerobjects.QuantityByPriorityAndResourceType)
	for _, cluster := range clusters {
		if cluster.Pool == pool {
			for queue, report := range cluster.Usage.ResourcesByQueue {
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
