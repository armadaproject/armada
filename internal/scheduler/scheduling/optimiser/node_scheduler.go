package optimiser

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/server/configuration"
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
	met, _, err := nodedb.StaticJobRequirementsMet(node, jctx)
	if err != nil {
		return nil, err
	}
	if !met {
		return &nodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
			scheduled: false,
		}, nil
	}

	availableResource := node.AllocatableByPriority[internaltypes.EvictedPriority]
	if !jctx.Job.AllResourceRequirements().Exceeds(availableResource) {
		return &nodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
			scheduled: true,
		}, nil
	}

	queues, err := n.getPreemptibleJobDetailsByQueue(schedContext, jctx, node)
	if err != nil {
		return nil, err
	}
	err = n.populateQueueImpactFields(schedContext, queues)
	if err != nil {
		return nil, err
	}

	allJobs := []*preemptibleJobDetails{}
	for _, queueItems := range queues {
		allJobs = append(allJobs, queueItems...)
	}
	sort.Sort(globalPreemptionOrder(allJobs))

	scheduled := false
	totalCost := float64(0)
	jobsToPreempt := []string{}
	queueCostChanges := map[string]float64{}
	for _, jobToEvict := range allJobs {
		availableResource = availableResource.Add(jobToEvict.resources)
		totalCost += jobToEvict.cost
		if _, present := queueCostChanges[jobToEvict.queue]; !present {
			queueCostChanges[jobToEvict.queue] = 0
		}
		queueCostChanges[jobToEvict.queue] -= jobToEvict.cost
		jobsToPreempt = append(jobsToPreempt, jobToEvict.jobId)

		if !jctx.Job.AllResourceRequirements().Exceeds(availableResource) {
			scheduled = true
			break
		}
	}

	maximumQueueImpact := float64(0)
	for queue, costChange := range queueCostChanges {
		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return nil, fmt.Errorf("could not find queue context for queue %s", queue)
		}

		// TODO This should work fine, but probably should be cost / original total cost
		// So if it a queue is way above its fairshare, this number reflects the % of current allocation lost, rather than % of fairshare
		impact := math.Abs(costChange) / qctx.Fairshare
		if impact > maximumQueueImpact {
			maximumQueueImpact = impact
		}
	}

	if !scheduled {
		return &nodeSchedulingResult{
			scheduled: false,
			jctx:      jctx,
			node:      node,
			resultId:  util.NewULID(),
		}, nil
	}

	return &nodeSchedulingResult{
		scheduled:          true,
		jctx:               jctx,
		node:               node,
		schedulingCost:     totalCost,
		jobIdsToPreempt:    jobsToPreempt,
		queueCostChanges:   queueCostChanges,
		maximumQueueImpact: maximumQueueImpact,
		resultId:           util.NewULID(),
	}, nil
}

func (n *NodeScheduler) getPreemptibleJobDetailsByQueue(
	schedContext *SchedulingContext,
	jobToSchedule *context.JobSchedulingContext,
	node *internaltypes.Node) (map[string][]*preemptibleJobDetails, error) {
	queues := map[string][]*preemptibleJobDetails{}
	for jobId, resource := range node.AllocatedByJobId {
		job := n.jobDb.GetById(jobId)
		if job == nil {
			return nil, fmt.Errorf("job %s not found in jobDb", jobId)
		}
		if !job.PriorityClass().Preemptible {
			// Don't evict non-preemptible jobs
			continue
		}
		if isTooLargeToEvict(job, n.maximumJobSizeToPreempt) {
			// Don't evict jobs larger than the maximum size
			continue
		}
		if isGang(job) {
			// Don't evict gang jobs
			continue
		}
		queue := job.Queue()
		var scheduledAtPriority int32
		age := int64(0)
		if job.Queued() {
			qctx, ok := schedContext.Sctx.QueueSchedulingContexts[queue]
			if !ok {
				return nil, fmt.Errorf("could not find queue context for queue %s", queue)
			}
			jctx, ok := qctx.SuccessfulJobSchedulingContexts[jobId]
			if !ok {
				return nil, fmt.Errorf("could not job context for job %s in successful scheduling contexts for queue %s, expected to find it scheduled on node %s", jobId, queue, node.GetName())
			}
			if jctx.PodSchedulingContext == nil {
				return nil, fmt.Errorf("no pod scheduling context exists on jctx for job %s despite it being in successful scheduling contexts", jobId)
			}
			scheduledAtPriority = jctx.PodSchedulingContext.ScheduledAtPriority
		} else {
			scheduledAtPriority = *job.LatestRun().ScheduledAtPriority()
			age = time.Now().Sub(*job.LatestRun().LeaseTime()).Milliseconds()
		}
		if scheduledAtPriority > jobToSchedule.Job.PriorityClass().Priority {
			// Can't evict jobs of higher priority
			continue
		}

		cost := schedContext.Sctx.FairnessCostProvider.UnweightedCostFromAllocation(resource)
		runInfo := &preemptibleJobDetails{
			cost:                cost,
			resources:           resource,
			jobId:               jobId,
			queue:               queue,
			scheduledAtPriority: scheduledAtPriority,
			ageMillis:           age,
		}

		if _, present := queues[queue]; !present {
			queues[queue] = []*preemptibleJobDetails{runInfo}
		} else {
			queues[queue] = append(queues[queue], runInfo)
		}
	}
	return queues, nil
}

func (n *NodeScheduler) populateQueueImpactFields(schedContext *SchedulingContext, queues map[string][]*preemptibleJobDetails) error {
	for queue, items := range queues {
		sort.Sort(internalQueueOrder(items))

		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return fmt.Errorf("queue context for queue %s not found", queue)
		}

		updatedQueueCost := qctx.CurrentCost
		count := 0
		for _, item := range items {
			item.queueCostAfterPreemption = updatedQueueCost - item.cost
			updatedQueueCost = item.queueCostAfterPreemption
			if item.queueCostAfterPreemption > qctx.Fairshare {
				item.costToPreempt = 0
			} else {
				// This could be improved to handle crossing the fairshare boundary better
				// It could just cost the amount the queue is brought below fairshare, rather than the full item cost
				item.costToPreempt = item.cost
			}
			item.queuePreemptedOrdinal = count
			count++
		}

		queues[queue] = items
	}
	return nil
}

func isTooLargeToEvict(job *jobdb.Job, limit *internaltypes.ResourceList) bool {
	if limit == nil {
		return false
	}
	if job == nil {
		return true
	}
	jobResources := job.AllResourceRequirements()
	if jobResources.Factory() != limit.Factory() {
		return true
	}
	for i, resource := range limit.GetResources() {
		if resource.Value.IsZero() {
			continue
		}
		result := resource.Value.Cmp(jobResources.GetResources()[i].Value)
		if result < 0 {
			return true
		}
	}
	return false
}

func isGang(job *jobdb.Job) bool {
	if job == nil {
		return false
	}
	gangCardinalityString, ok := job.Annotations()[configuration.GangCardinalityAnnotation]
	if !ok {
		return false
	}
	gangCardinality, err := strconv.Atoi(gangCardinalityString)
	if err != nil {
		logging.WithStacktrace(err).Errorf("unexpected value in gang cardinality annotation")
		return false
	}
	return gangCardinality > 1
}
