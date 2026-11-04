package optimiser

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type PreemptingNodeScheduler struct {
	jobDb                   jobdb.JobRepository
	nodeDb                  *nodedb.NodeDb
	maximumJobSizeToPreempt *internaltypes.ResourceList
	allowGangPreemption     bool
}

type NodeScheduler interface {
	Schedule(schedContext *SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error)
}

func NewPreemptingNodeScheduler(jobDb jobdb.JobRepository, nodeDb *nodedb.NodeDb, maximumJobSizeToPreempt *internaltypes.ResourceList, allowGangPreemption bool) *PreemptingNodeScheduler {
	return &PreemptingNodeScheduler{
		jobDb:                   jobDb,
		nodeDb:                  nodeDb,
		maximumJobSizeToPreempt: maximumJobSizeToPreempt,
		allowGangPreemption:     allowGangPreemption,
	}
}

// Schedule
// This function is responsible for determining if a job can be scheduled on a node, and the cost to do so
// High level steps:
// - Determine the jobs or gangs that can be preempted
// - Order these jobs in the ideal order to preempt them in
//   - Group these jobs by queue and order them by the order that queue would want those jobs ordered
//   - This will give you N ordered lists, one of each queue
//   - Combine these ordered lists into a "global" order, which is the ideal order to preempt jobs in
//
// - Attempt to schedule the new job on the node, preempting one gang at a time until the job fits
func (n *PreemptingNodeScheduler) Schedule(schedContext *SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error) {
	met, _, err := nodedb.StaticJobRequirementsMet(node, jctx)
	if err != nil {
		return nil, err
	}
	if !met {
		return &nodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  node.GetId(),
			scheduled: false,
		}, nil
	}

	availableResource := node.AllocatableAtPriority(internaltypes.EvictedPriority)
	if !jctx.Job.KubernetesResourceRequirements().Exceeds(availableResource) {
		return &nodeSchedulingResult{
			jctx:      jctx,
			node:      node,
			resultId:  node.GetId(),
			scheduled: true,
		}, nil
	}

	if schedContext.NodeDbTxn == nil {
		return nil, fmt.Errorf("node db transaction is nil")
	}
	queues, err := n.getPreemptibleGangsByQueue(schedContext, jctx, node)
	if err != nil {
		return nil, err
	}
	err = n.populateQueueImpactFields(schedContext, jctx, queues)
	if err != nil {
		return nil, err
	}

	allGangs := []*preemptibleGang{}
	for _, queueItems := range queues {
		allGangs = append(allGangs, queueItems...)
	}
	sort.Sort(globalPreemptionOrder(allGangs))

	scheduled := false
	totalCost := float64(0)
	gangsToPreempt := []*preemptibleGang{}
	resourcesPreemptedByQueue := map[string]internaltypes.ResourceList{}
	for _, gangToPreempt := range allGangs {
		availableResource = availableResource.Add(gangToPreempt.applicableResources)
		totalCost += gangToPreempt.costToPreempt
		if _, ok := resourcesPreemptedByQueue[gangToPreempt.queue]; !ok {
			resourcesPreemptedByQueue[gangToPreempt.queue] = internaltypes.ResourceList{}
		}
		resourcesPreemptedByQueue[gangToPreempt.queue] = resourcesPreemptedByQueue[gangToPreempt.queue].Add(gangToPreempt.totalResources)
		gangsToPreempt = append(gangsToPreempt, gangToPreempt)

		if !jctx.Job.KubernetesResourceRequirements().Exceeds(availableResource) {
			scheduled = true
			break
		}
	}

	queueCostChanges := map[string]float64{}
	maximumQueueImpact := float64(0)
	for queue, preemptedResource := range resourcesPreemptedByQueue {
		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return nil, fmt.Errorf("could not find queue context for queue %s", queue)
		}

		newCost := schedContext.Sctx.FairnessCostProvider.UnweightedCostFromAllocation(qctx.AllocationInclShortJobPenalty.Subtract(preemptedResource))
		costChange := math.Abs(qctx.CurrentCost - newCost)
		if _, ok := queueCostChanges[qctx.Name]; !ok {
			queueCostChanges[qctx.Name] = 0
		}
		queueCostChanges[qctx.Name] -= costChange

		impact := float64(0)
		if qctx.CurrentCost > 0 {
			impact = costChange / qctx.CurrentCost
		}
		if impact > maximumQueueImpact {
			maximumQueueImpact = impact
		}
	}

	if !scheduled {
		return &nodeSchedulingResult{
			scheduled: false,
			jctx:      jctx,
			node:      node,
			resultId:  node.GetId(),
		}, nil
	}

	return &nodeSchedulingResult{
		scheduled:          true,
		jctx:               jctx,
		node:               node,
		schedulingCost:     totalCost,
		gangsToPreempt:     gangsToPreempt,
		queueCostChanges:   queueCostChanges,
		maximumQueueImpact: maximumQueueImpact,
		resultId:           node.GetId(),
	}, nil
}

func (n *PreemptingNodeScheduler) getPreemptibleGangsByQueue(
	schedContext *SchedulingContext,
	jobToSchedule *context.JobSchedulingContext,
	node *internaltypes.Node,
) (map[string][]*preemptibleGang, error) {
	queues := map[string][]*preemptibleGang{}
	start := time.Now()
	seen := make(map[string]bool)
	for jobId := range node.AllocatedByJob() {
		if seen[jobId] {
			continue
		}
		job := n.jobDb.GetById(jobId)
		if job == nil {
			return nil, fmt.Errorf("job %s not found in jobDb", jobId)
		}
		if !n.isJobPreemptionCandidate(job, node) {
			continue
		}
		jobs := []*jobdb.Job{job}
		if job.IsInGang() {
			if !n.allowGangPreemption {
				continue
			}
			if job.Queue() == jobToSchedule.Job.Queue() && job.GetGangInfo().Id() == jobToSchedule.Job.GetGangInfo().Id() {
				// Members placed earlier in this attempt must stay scheduled.
				continue
			}
			var err error
			jobs, err = n.jobDb.GetGangJobsByGangId(job.Queue(), job.GetGangInfo().Id())
			if err != nil {
				return nil, err
			}
		}
		// Several members on this node must still count as one gang.
		for _, member := range jobs {
			seen[member.Id()] = true
		}
		gang, err := n.getPreemptibleGang(schedContext, jobToSchedule, job, jobs, node, start)
		if err != nil {
			return nil, err
		}
		if gang != nil {
			queues[gang.queue] = append(queues[gang.queue], gang)
		}
	}
	return queues, nil
}

// getPreemptibleGang constructs a preemptibleGang from individual member jobs.
func (n *PreemptingNodeScheduler) getPreemptibleGang(
	schedContext *SchedulingContext,
	jobToSchedule *context.JobSchedulingContext,
	job *jobdb.Job,
	jobs []*jobdb.Job,
	node *internaltypes.Node,
	now time.Time,
) (*preemptibleGang, error) {
	queue := job.Queue()
	crossPool := !context.IsHomeJob(job, schedContext.Sctx.Pool)
	if crossPool {
		queue = context.CalculateAwayQueueName(queue)
	}
	qctx, ok := schedContext.Sctx.QueueSchedulingContexts[queue]
	if !ok {
		return nil, fmt.Errorf("could not find queue context for queue %s", queue)
	}
	gang := &preemptibleGang{
		preemptedSiblingJobId: job.Id(),
		queue:                 queue,
		crossPool:             crossPool,
		scheduledAtPriority:   internaltypes.MinPriority,
	}
	for _, member := range jobs {
		// This round's placement takes precedence over a previous run's node.
		nodeId := ""
		var scheduledAtPriority int32

		if crossPool {
			if member.LatestRun() == nil {
				return nil, fmt.Errorf("no job run found for cross pool job %s", member.Id())
			}

			nodeId = member.LatestRun().NodeId()
			scheduledAtPriority = internaltypes.CrossPoolPriority
		} else if member.Queued() {
			jctx, ok := qctx.SuccessfulJobSchedulingContexts[member.Id()]
			if !ok {
				return nil, fmt.Errorf("could not find job context for job %s in successful scheduling contexts for queue %s, expected to find it scheduled on node %s", member.Id(), queue, node.GetName())
			}
			if jctx.PodSchedulingContext == nil {
				return nil, fmt.Errorf("no pod scheduling context exists on jctx for job %s despite it being in successful scheduling contexts", member.Id())
			}

			nodeId = jctx.PodSchedulingContext.NodeId
			scheduledAtPriority = jctx.PodSchedulingContext.ScheduledAtPriority
		} else {
			if member.LatestRun() == nil {
				return nil, fmt.Errorf("no job run found for scheduled job %s", member.Id())
			}
			if member.LatestRun().ScheduledAtPriority() == nil {
				return nil, fmt.Errorf("scheduled at priority is nil for scheduled job %s", member.Id())
			}

			nodeId = member.LatestRun().NodeId()
			scheduledAtPriority = *member.LatestRun().ScheduledAtPriority()
		}

		memberNode, err := n.nodeDb.GetNodeWithTxn(schedContext.NodeDbTxn, nodeId)
		if err != nil {
			return nil, err
		}
		// NodeDB can return nil nodes for cordoned executors.
		if !n.isJobPreemptionCandidate(member, memberNode) || memberNode == nil {
			return nil, nil
		}
		if scheduledAtPriority > jobToSchedule.Job.PriorityClass().Priority {
			return nil, nil
		}
		jctx := context.JobSchedulingContextFromJob(member)
		// Record the original node on this new context for unbinding and preemption details.
		jctx.AssignedNode = memberNode
		gang.jobs = append(gang.jobs, jctx)
		gang.scheduledAtPriority = max(gang.scheduledAtPriority, scheduledAtPriority)
		if !member.Queued() && member.LatestRun() != nil && member.LatestRun().LeaseTime() != nil {
			gang.ageMillis = max(gang.ageMillis, now.Sub(*member.LatestRun().LeaseTime()).Milliseconds())
		}
		gang.totalResources = gang.totalResources.Add(member.AllResourceRequirements())
		if scheduledAtPriority < jobToSchedule.Job.PriorityClass().Priority {
			gang.lowerPriorityResources = gang.lowerPriorityResources.Add(member.AllResourceRequirements())
		}
		if nodeId == node.GetId() {
			gang.applicableResources = gang.applicableResources.Add(member.KubernetesResourceRequirements())
		}
	}
	if len(gang.jobs) == 0 {
		return nil, nil
	}
	gang.cost = schedContext.Sctx.FairnessCostProvider.UnweightedCostFromAllocation(gang.totalResources)
	return gang, nil
}

func (n *PreemptingNodeScheduler) populateQueueImpactFields(schedContext *SchedulingContext, jobToSchedule *context.JobSchedulingContext, queues map[string][]*preemptibleGang) error {
	for queue, items := range queues {
		sort.Sort(internalQueueOrder(items))

		qctx, ok := schedContext.Queues[queue]
		if !ok {
			return fmt.Errorf("queue context for queue %s not found", queue)
		}

		allocation := qctx.AllocationInclShortJobPenalty
		count := 0
		for _, item := range items {
			item.costToPreempt = item.unweightedPreemptionCost(schedContext, allocation)
			allocation = allocation.Subtract(item.totalResources)
			after := schedContext.Sctx.FairnessCostProvider.UnweightedCostFromAllocation(allocation)
			item.weightedCostAfterPreemption = after / qctx.Weight
			if !item.crossPool && item.scheduledAtPriority < jobToSchedule.Job.PriorityClass().Priority {
				item.priorityPreemption = true
			}
			item.queuePreemptedOrdinal = count
			count++
		}

		queues[queue] = items
	}
	return nil
}

func (n *PreemptingNodeScheduler) isJobPreemptionCandidate(job *jobdb.Job, allocatedNode *internaltypes.Node) bool {
	if allocatedNode != nil && allocatedNode.IsJobEvicted(job.Id()) {
		return false
	}
	if job.InTerminalState() {
		return false
	}
	if !job.PriorityClass().Preemptible {
		// Don't evict non-preemptible jobs
		return false
	}
	if isTooLargeToEvict(job, n.maximumJobSizeToPreempt) {
		// Don't evict jobs larger than the maximum size
		return false
	}
	return true
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
		panic("mismatched ResourceListFactory in node_scheduler")
	}
	for i, resource := range limit.GetAll() {
		if resource.Value.IsZero() {
			continue
		}
		result := resource.Value.Cmp(jobResources.GetAll()[i].Value)
		if result < 0 {
			return true
		}
	}
	return false
}
