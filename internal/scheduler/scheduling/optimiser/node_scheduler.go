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

type PreemptingNodeScheduler struct {
	jobDb                   jobdb.JobRepository
	maximumJobSizeToPreempt *internaltypes.ResourceList
}

type NodeScheduler interface {
	Schedule(schedContext *SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error)
}

func NewPreemptingNodeScheduler(jobDb jobdb.JobRepository, maximumJobSizeToPreempt *internaltypes.ResourceList) *PreemptingNodeScheduler {
	return &PreemptingNodeScheduler{
		jobDb:                   jobDb,
		maximumJobSizeToPreempt: maximumJobSizeToPreempt,
	}
}

// Schedule
// This function is responsible for determining if a job can be scheduled on a node, and the cost to do so
// High level steps:
// - Determine all the jobs that can be preempted
// - Order these jobs in the ideal order to preempt them in
//   - Group these jobs by queue and order them by the order that queue would want those jobs ordered
//   - This will give you N ordered lists, one of each queue
//   - Combine these ordered lists into a "global" order, which is the ideal order to preempt jobs in
//
// - Attempt to schedule the new job on the node, preempting one job at a time until the job fit
func (n *PreemptingNodeScheduler) Schedule(schedContext *SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error) {
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
	err = n.populateQueueImpactFields(schedContext, jctx, queues)
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
		totalCost += jobToEvict.costToPreempt
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

		impact := math.Abs(costChange) / qctx.CurrentCost
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

func (n *PreemptingNodeScheduler) getPreemptibleJobDetailsByQueue(
	schedContext *SchedulingContext,
	jobToSchedule *context.JobSchedulingContext,
	node *internaltypes.Node,
) (map[string][]*preemptibleJobDetails, error) {
	queues := map[string][]*preemptibleJobDetails{}
	start := time.Now()
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
				return nil, fmt.Errorf("could not find job context for job %s in successful scheduling contexts for queue %s, expected to find it scheduled on node %s", jobId, queue, node.GetName())
			}
			if jctx.PodSchedulingContext == nil {
				return nil, fmt.Errorf("no pod scheduling context exists on jctx for job %s despite it being in successful scheduling contexts", jobId)
			}
			scheduledAtPriority = jctx.PodSchedulingContext.ScheduledAtPriority
		} else {
			if job.LatestRun() == nil {
				return nil, fmt.Errorf("no job run found for scheduled job %s", jobId)
			}
			if job.LatestRun().ScheduledAtPriority() == nil {
				return nil, fmt.Errorf("scheduled at priority is nil for scheduled job %s", jobId)
			}
			scheduledAtPriority = *job.LatestRun().ScheduledAtPriority()
			age = start.Sub(*job.LatestRun().LeaseTime()).Milliseconds()
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

func (n *PreemptingNodeScheduler) populateQueueImpactFields(schedContext *SchedulingContext, jobToSchedule *context.JobSchedulingContext, queues map[string][]*preemptibleJobDetails) error {
	for queue, items := range queues {
		sort.Sort(internalQueueOrder(items))

		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return fmt.Errorf("queue context for queue %s not found", queue)
		}

		updatedQueueCost := qctx.CurrentCost
		count := 0
		for _, item := range items {
			updatedQueueCost = roundFloatHighPrecision(updatedQueueCost - item.cost)
			item.weightedCostAfterPreemption = updatedQueueCost / qctx.Weight
			if item.scheduledAtPriority < jobToSchedule.Job.PriorityClass().Priority {
				item.costToPreempt = 0
				item.priorityPreemption = true
			} else if updatedQueueCost > qctx.Fairshare {
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

func roundFloatHighPrecision(input float64) float64 {
	return math.Round(input*100000000) / 100000000
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
