package optimiser

import (
	"fmt"
	"sort"
	"time"

	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type NodeScheduler struct {
	jobDb                   jobdb.JobRepository
	maximumJobSizeToPreempt *internaltypes.ResourceList
}

func NewNodeScheduler(jobDb jobdb.JobRepository, maximumJobSizeToPreempt *internaltypes.ResourceList) *NodeScheduler {
	return &NodeScheduler{
		jobDb:                   jobDb,
		maximumJobSizeToPreempt: maximumJobSizeToPreempt,
	}
}

func (n *NodeScheduler) Schedule(schedContext *SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error) {
	queues := map[string][]*runPreemptionInfo{}
	met, _, err := nodedb.StaticJobRequirementsMet(node, jctx)

	if err != nil {
		return nil, err
	}
	if !met {
		return &nodeSchedulingResult{
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
		if n.maximumJobSizeToPreempt != nil && job.AllResourceRequirements().Exceeds(*n.maximumJobSizeToPreempt) {
			// Don't evict jobs larger than the maximum size
			continue
		}
		// TODO prevent gang jobs being evicted
		queue := job.Queue()
		var scheduledAtPriority int32
		age := int64(0)
		if job.Queued() {
			// TODO handle edge cases here
			scheduledAtPriority = schedContext.Sctx.QueueSchedulingContexts[queue].SuccessfulJobSchedulingContexts[jobId].PodSchedulingContext.ScheduledAtPriority
		} else {
			scheduledAtPriority = *job.LatestRun().ScheduledAtPriority()
			age = time.Now().Sub(*job.LatestRun().LeaseTime()).Milliseconds()
		}
		if scheduledAtPriority > jctx.Job.PriorityClass().Priority {
			// Can't evict jobs of higher priority
			continue
		}

		cost := schedContext.Sctx.FairnessCostProvider.UnweightedCostFromAllocation(resource)

		runInfo := &runPreemptionInfo{
			cost:                cost,
			resources:           resource,
			jobId:               jobId,
			queue:               queue,
			scheduledAtPriority: scheduledAtPriority,
			age:                 age,
		}

		if _, present := queues[queue]; !present {
			queues[queue] = []*runPreemptionInfo{runInfo}
		} else {
			queues[queue] = append(queues[queue], runInfo)
		}
	}

	for queue, items := range queues {
		// TODO confirm this actually sorts in place
		sort.Sort(internalQueueOrder(items))

		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return nil, fmt.Errorf("queue context for queue %s not found", queue)
		}

		updatedQueueCost := qctx.CurrentCost
		count := 0
		for _, item := range items {
			item.queueCostAfterPreemption = updatedQueueCost - item.cost
			updatedQueueCost = item.queueCostAfterPreemption
			item.costAsPercentageOfQueueShare = (item.cost / qctx.Fairshare) * 100
			if item.queueCostAfterPreemption > qctx.Fairshare {
				item.costToPreempt = 0
				item.costAsPercentageOfQueueShare = 0
			}
			item.queuePreemptedOrdinal = count
			count++
		}

		queues[queue] = items
	}

	allJobs := []*runPreemptionInfo{}

	for _, queueItems := range queues {
		allJobs = append(allJobs, queueItems...)
	}
	sort.Sort(globalPreemptionOrder(allJobs))

	availableResource := node.AllocatableByPriority[internaltypes.EvictedPriority]
	if !jctx.Job.AllResourceRequirements().Exceeds(availableResource) {
		return &nodeSchedulingResult{
			scheduled: true,
		}, nil
	}

	scheduled := false
	totalCost := float64(0)
	// TODO queue impact is confused
	// Should be a sum of total impact over a queue
	maximumQueueImpact := float64(0)
	jobsToPreempt := []string{}
	queueCostChanges := map[string]float64{}
	for _, jobToEvict := range allJobs {
		availableResource = availableResource.Add(jobToEvict.resources)
		totalCost += jobToEvict.cost
		if _, present := queueCostChanges[jobToEvict.queue]; !present {
			queueCostChanges[jobToEvict.queue] = 0
		}
		queueCostChanges[jobToEvict.queue] -= jobToEvict.cost
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
		return &nodeSchedulingResult{
			scheduled: false,
		}, nil
	}

	return &nodeSchedulingResult{
		scheduled:        true,
		node:             node,
		schedulingCost:   totalCost,
		jobIdsToPreempt:  jobsToPreempt,
		queueCostChanges: queueCostChanges,
		resultId:         util.NewULID(),
	}, nil
}
