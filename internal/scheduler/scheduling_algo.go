package scheduler

import (
	"context"
	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/scheduler/database"
	"github.com/G-Research/armada/internal/scheduler/schedulerobjects"
	"github.com/hashicorp/go-memdb"
	"time"
)

// SchedulingAlgo is an interface that should bne implemented by structs capable of assigning Jobs to nodes
type SchedulingAlgo interface {
	// Schedule should assign jobs to nodes
	// Any jobs that are scheduled should be marked as such in the JobDb using the transaction provided
	// It should return a slice containing all scheduled jobs.
	Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error)
}

type LegacySchedulingAlgo struct {
	config             *configuration.SchedulingConfig
	executorRepository database.ExecutorRepository
	queueRepository    database.QueueRepository
	priorities         []int32
	indexedResources   []string
}

func NewLegacySchedulingAlgo(config *configuration.SchedulingConfig) {
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
}

func (l *LegacySchedulingAlgo) Schedule(txn *memdb.Txn, jobDb *JobDb) ([]*SchedulerJob, error) {

	jobsToSchedule := make([]*SchedulerJob, 0)

	clusters, err := l.executorRepository.GetExecutors()
	if err != nil {
		return nil, err
	}

	// TODO: filter out queues that don't have any jobs
	queues, err := l.queueRepository.GetAllQueues()
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

	for _, cluster := range clusters {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		initialResourcesByQueueAndPriority := aggregateUsage(clusters, cluster.Pool)

		jobRepo := NewJobDbAdapter(txn)
		nodeDb, err := l.constructNodeDb(cluster.Nodes, l.priorities)
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

		if err != nil {
			return nil, err
		}
		updatedJobs := make([]*SchedulerJob, len(jobs))
		for _, report := range legacyScheduler.SchedulingRoundReport.SuccessfulJobSchedulingReports() {
			copy := report.Job.DeepCopy()
			copy.Leased = true
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
