package scheduling

import (
	"fmt"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type NodeScheduler struct {
	jobDb        jobdb.JobRepository
	nodeDb       *nodedb.NodeDb
	defragConfig configuration.DefragConfig
}

func NewNodeScheduler(jobDb jobdb.JobRepository, nodeDb *nodedb.NodeDb, defragConfig configuration.DefragConfig) *NodeScheduler {
	return &NodeScheduler{
		nodeDb:       nodeDb,
		jobDb:        jobDb,
		defragConfig: defragConfig,
	}
}

// TODO also need current queue costs
func (n *NodeScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, string, error) {
	nodes, err := n.nodeDb.GetNodes()
	if err != nil {
		// TODO message
		return false, "", err
	}
	// Pre-filter nodes?
	// Split nodes into groups based on gangUniformityLabel + compare at the end

	// TODO
	// Handle gangs
	// Handle gang uniformity label
	// Sorting
	for _, jctx := range gctx.JobSchedulingContexts {
		candidateNodes := make([]*evictionResult, 0, len(nodes))
		cost := sctx.FairnessCostProvider.UnweightedCostFromAllocation(jctx.Job.AllResourceRequirements())
		for _, node := range nodes {

			result, err := n.calculateCostToScheduleOnNode(sctx, jctx, node)

			if err != nil {
				// TODO message
				return false, "", err
			}

			if result.scheduled && result.info.totalEvictionCost < cost {
				candidateNodes = append(candidateNodes, result)
				if result.info.totalEvictionCost == 0 {
					break
				}

			}
		}

		if len(candidateNodes) == 0 {
			// TODO message
			return false, "", nil
		}

		// TODO
		// Sort nodes by cost
		// Average age of evicted jobs
		// Maximum % impact on queue
		// Tie break on queue name
		// slices.sort(candidateNodes)

		result := candidateNodes[0]

		jobsToPreempt := make([]*jobdb.Job, 0, len(result.info.jobIdsToPreempt))

		for _, jobId := range result.info.jobIdsToPreempt {
			// Handle not found
			jobsToPreempt = append(jobsToPreempt, n.jobDb.GetById(jobId))
		}

		node, err := n.nodeDb.UnbindJobsFromNode(jobsToPreempt, result.node)
		if err != nil {
			// TODO message
			return false, "", err
		}
		// TODO update sctx, make pod scheduling context etc
		node, err = n.nodeDb.BindJobToNode(node, jctx.Job, jctx.Job.PriorityClass().Priority)
		if err != nil {
			// TODO message
			return false, "", err
		}

		err = n.nodeDb.Upsert(node)
		if err != nil {
			// TODO message
			return false, "", err
		}

		// TODO sort out sctx prempted/scheduled

		return true, "", nil

	}

	return false, "no scheduling done", nil
}

type runEvictionInfo struct {
	cost          float64
	costToPreempt float64
	// TODO make pointer
	resources           internaltypes.ResourceList
	jobId               string
	queue               string
	scheduledAtPriority int32
	age                 int64
	// Used for global ordering
	queueCostAfterPreemption     float64
	costAsPercentageOfQueueShare float64
}

type evictionResult struct {
	scheduled bool
	node      *internaltypes.Node
	info      *evictionResultInfo
}

type evictionResultInfo struct {
	totalEvictionCost  float64
	jobIdsToPreempt    []string
	maximumQueueImpact float64
	// Used to tie-break when sorting
	resultId string
}

func (n *NodeScheduler) calculateCostToScheduleOnNode(sctx *context.SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*evictionResult, error) {

	queues := map[string][]*runEvictionInfo{}
	met, _, err := nodedb.StaticJobRequirementsMet(node, jctx)

	if err != nil {
		return nil, err
	}
	if !met {
		return &evictionResult{
			scheduled: false,
		}, nil
	}

	for jobId, resource := range node.AllocatedByJobId {
		job := n.jobDb.GetById(jobId)
		if job == nil {
			return nil, fmt.Errorf("job %s not found in jobDb", jobId)
		}
		if !job.PriorityClass().Preemptible {
			// Don't evict non-preemptible jobs
			continue
		}
		queue := job.Queue()
		var scheduledAtPriority int32
		age := int64(0)
		if job.Queued() {
			// TODO handle edge cases here
			scheduledAtPriority = sctx.QueueSchedulingContexts[queue].SuccessfulJobSchedulingContexts[jobId].PodSchedulingContext.ScheduledAtPriority
		} else {
			scheduledAtPriority = *job.LatestRun().ScheduledAtPriority()
			age = time.Now().Sub(*job.LatestRun().LeaseTime()).Milliseconds()
		}

		cost := sctx.FairnessCostProvider.UnweightedCostFromAllocation(resource)

		runInfo := &runEvictionInfo{
			cost:                cost,
			resources:           resource,
			jobId:               jobId,
			queue:               queue,
			scheduledAtPriority: scheduledAtPriority,
			age:                 age,
		}

		if scheduledAtPriority > jctx.Job.PriorityClass().Priority {
			// Can't evict jobs of higher priority
			continue
		}

		if _, present := queues[queue]; !present {
			queues[queue] = []*runEvictionInfo{runInfo}
		} else {
			queues[queue] = append(queues[queue], runInfo)
		}
	}

	for queue, items := range queues {
		// TODO sort
		// slices.Sort(items)

		queueShare := sctx.FairnessCostProvider.UnweightedCostFromQueue(sctx.QueueSchedulingContexts[queue])
		originalFairshare := sctx.QueueSchedulingContexts[queue].AdjustedFairShare
		fairshare := sctx.QueueSchedulingContexts[queue].AdjustedFairShare

		for _, item := range items {
			item.queueCostAfterPreemption = queueShare - item.cost
			item.costAsPercentageOfQueueShare = (item.cost / originalFairshare) * 100
			if item.queueCostAfterPreemption > fairshare {
				item.costToPreempt = 0
				item.costAsPercentageOfQueueShare = 0
			}
		}

		queues[queue] = items
	}

	allJobs := []*runEvictionInfo{}

	for _, queueItems := range queues {
		allJobs = append(allJobs, queueItems...)
	}
	// TODO sort
	// slices.Sort(allJobs)

	availableResource := node.AllocatableByPriority[internaltypes.EvictedPriority]
	if !jctx.Job.AllResourceRequirements().Exceeds(availableResource) {
		return &evictionResult{
			scheduled: true,
		}, nil
	}

	scheduled := false
	totalCost := float64(0)
	// TODO queue impact is confused
	// Should be a sum of total impact over a queue
	maximumQueueImpact := float64(0)
	jobsToPreempt := []string{}
	for _, jobToEvict := range allJobs {
		availableResource = availableResource.Add(jobToEvict.resources)
		totalCost += jobToEvict.cost
		if jobToEvict.costAsPercentageOfQueueShare > maximumQueueImpact {
			maximumQueueImpact = jobToEvict.costAsPercentageOfQueueShare
		}
		jobsToPreempt = append(jobsToPreempt, jobToEvict.jobId)

		if !jctx.Job.AllResourceRequirements().Exceeds(availableResource) {
			scheduled = true
			break
		}
	}

	if !scheduled {
		return &evictionResult{
			scheduled: false,
		}, nil
	}

	return &evictionResult{
		scheduled: true,
		node:      node,
		info: &evictionResultInfo{
			totalEvictionCost: totalCost,
			jobIdsToPreempt:   jobsToPreempt,
			resultId:          util.NewULID(),
		},
	}, nil
}
