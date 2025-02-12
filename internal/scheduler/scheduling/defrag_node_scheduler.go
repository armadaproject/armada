package scheduling

import (
	"fmt"
	"sort"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/util"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/optimiser/domain"
)

type NodeScheduler struct {
	jobDb                        jobdb.JobRepository
	nodeDb                       *nodedb.NodeDb
	factory                      *internaltypes.ResourceListFactory
	fairnessImprovementThreshold float64
	maximumJobSizeToPreempt      *internaltypes.ResourceList
}

func NewNodeScheduler(
	jobDb jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
	fairnessImprovementThreshold float64,
	maximumJobSizeToPreempt *internaltypes.ResourceList) *NodeScheduler {
	return &NodeScheduler{
		nodeDb:                       nodeDb,
		jobDb:                        jobDb,
		fairnessImprovementThreshold: fairnessImprovementThreshold,
		maximumJobSizeToPreempt:      maximumJobSizeToPreempt,
	}
}

func (n *NodeScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error) {
	nodes, err := n.nodeDb.GetNodes()
	if err != nil {
		return false, nil, "", err
	}
	// Pre-filter nodes?

	if isValid, reason := n.isValidNodeUniformityLabel(gctx.NodeUniformity); !isValid {
		return false, nil, reason, nil
	}

	nodesByNodeUniformityLabel := n.groupNodesByNodeUniformityLabel(gctx.NodeUniformity, nodes)
	schedulingCandidates := make([]*schedulingResult, 0, len(nodesByNodeUniformityLabel))

	for _, groupedNodes := range nodesByNodeUniformityLabel {
		result, err := n.scheduleOnNodes(gctx, sctx, groupedNodes)
		if err != nil {
			return false, nil, "", err
		}
		if result.scheduled {
			schedulingCandidates = append(schedulingCandidates, result)
		}
	}

	if len(schedulingCandidates) == 0 {
		return false, nil, "no optimised scheduling options found", nil
	}

	// sort candidates

	allPreemptedJobs, err := n.updateState(schedulingCandidates[0], sctx)
	if err != nil {
		return false, nil, "", err
	}
	return true, allPreemptedJobs, "", nil
}

func (n *NodeScheduler) scheduleOnNodes(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, nodes []*internaltypes.Node) (*schedulingResult, error) {
	result := &schedulingResult{
		scheduled:      true,
		schedulingCost: 0,
		results:        make([]*nodeSchedulingResult, 0, len(gctx.JobSchedulingContexts)),
	}

	schedulingContext := domain.FromSchedulingContext(sctx)

	for _, jctx := range gctx.JobSchedulingContexts {
		candidateNodes := make([]*nodeSchedulingResult, 0, len(nodes))
		cost := sctx.FairnessCostProvider.UnweightedCostFromAllocation(jctx.Job.AllResourceRequirements())
		for _, node := range nodes {
			result, err := n.calculateCostToScheduleOnNode(schedulingContext, jctx, node)

			if err != nil {
				return nil, err
			}

			if result.scheduled && cost/result.info.schedulingCost < n.fairnessImprovementThreshold {
				candidateNodes = append(candidateNodes, result)
				if result.info.schedulingCost == 0 {
					break
				}
			}
		}

		if len(candidateNodes) == 0 {
			return &schedulingResult{scheduled: false}, nil
		}

		sort.Sort(nodeCostOrder(candidateNodes))
		selectedCandidate := candidateNodes[0]
		result.results = append(result.results, selectedCandidate)
		result.schedulingCost += selectedCandidate.info.schedulingCost

		// Update nodes
		updatedNode := selectedCandidate.node.DeepCopyNilKeys()
		jobsToPreempt := make([]*jobdb.Job, 0, len(selectedCandidate.info.jobIdsToPreempt))

		for _, jobId := range selectedCandidate.info.jobIdsToPreempt {
			job := n.jobDb.GetById(jobId)
			if job == nil {
				return nil, fmt.Errorf("failed to find job %s in job db", jobId)
			}
			jobsToPreempt = append(jobsToPreempt, job)
		}

		updatedNode, err := n.nodeDb.UnbindJobsFromNode(jobsToPreempt, updatedNode)
		if err != nil {
			return nil, err
		}
		updatedNode, err = n.nodeDb.BindJobToNode(updatedNode, jctx.Job, jctx.Job.PriorityClass().Priority)
		if err != nil {
			return nil, err
		}

		for i, n := range nodes {
			if n.GetId() == updatedNode.GetId() {
				nodes[i] = updatedNode
				break
			}
		}

		for queueName, costChange := range selectedCandidate.info.queueCostChanges {
			queue, ok := schedulingContext.Queues[queueName]
			if !ok {
				return nil, fmt.Errorf("failed to find queue context for queue %s", queueName)
			}
			queue.CurrentCost += costChange
		}
	}
	return result, nil
}

func (n *NodeScheduler) updateState(result *schedulingResult, sctx *context.SchedulingContext) ([]*context.JobSchedulingContext, error) {
	// TODO perform in transaction
	allPreemptedJobs := []*context.JobSchedulingContext{}
	for _, result := range result.results {
		jctx := result.jctx
		jobsToPreempt := make([]*jobdb.Job, 0, len(result.info.jobIdsToPreempt))

		for _, jobId := range result.info.jobIdsToPreempt {
			job := n.jobDb.GetById(jobId)
			if job == nil {
				return nil, fmt.Errorf("failed to find job %s in job db", jobId)
			}
			jobsToPreempt = append(jobsToPreempt, job)
		}

		node, err := n.nodeDb.UnbindJobsFromNode(jobsToPreempt, result.node)
		if err != nil {
			return nil, err
		}
		node, err = n.nodeDb.BindJobToNode(node, jctx.Job, jctx.Job.PriorityClass().Priority)
		if err != nil {
			return nil, err
		}

		err = n.nodeDb.Upsert(node)
		if err != nil {
			return nil, err
		}

		preemptedJobs := make([]*context.JobSchedulingContext, 0, len(jobsToPreempt))
		allPreemptedJobs = append(allPreemptedJobs, preemptedJobs...)
		for _, jobToPreempt := range jobsToPreempt {
			jctx := context.JobSchedulingContextFromJob(jobToPreempt)
			jctx.PreemptingJobId = jctx.JobId
			jctx.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using fairness optimiser - preempting job %s", jctx.JobId)
			preemptedJobs = append(preemptedJobs, jctx)

			_, err = sctx.PreemptJob(jctx)
			if err != nil {
				return nil, err
			}
		}

		pctx := &context.PodSchedulingContext{
			Created:             time.Now(),
			ScheduledAtPriority: jctx.Job.PriorityClass().Priority,
			PreemptedAtPriority: internaltypes.MinPriority,
			NumNodes:            n.nodeDb.NumNodes(),
			SchedulingMethod:    context.ScheduledWithFairnessOptimiser,
		}
		jctx.PodSchedulingContext = pctx
		jctx.UnschedulableReason = ""
		_, err = sctx.AddJobSchedulingContext(jctx)
		if err != nil {
			return nil, err
		}
	}
	return allPreemptedJobs, nil
}

func (n *NodeScheduler) groupNodesByNodeUniformityLabel(nodeUniformityLabel string, nodes []*internaltypes.Node) map[string][]*internaltypes.Node {
	if nodeUniformityLabel == "" {
		return map[string][]*internaltypes.Node{"default": nodes}
	}

	result := map[string][]*internaltypes.Node{}
	for _, node := range nodes {
		if value, ok := node.GetLabelValue(nodeUniformityLabel); ok {
			if _, present := result[value]; !present {
				result[value] = []*internaltypes.Node{}
			}
			result[value] = append(result[value], node)
		}
	}

	return result
}

func (n *NodeScheduler) isValidNodeUniformityLabel(nodeUniformityLabel string) (bool, string) {
	if nodeUniformityLabel == "" {
		return true, ""
	}
	nodeUniformityLabelValues, ok := n.nodeDb.IndexedNodeLabelValues(nodeUniformityLabel)
	if !ok {
		return false, fmt.Sprintf("uniformity label %s is not indexed", nodeUniformityLabel)
	}
	if len(nodeUniformityLabelValues) == 0 {
		return false, fmt.Sprintf("no nodes with uniformity label %s", nodeUniformityLabel)
	}
	return true, ""
}

type runEvictionInfo struct {
	// metadata
	jobId string
	queue string
	// TODO make pointer
	resources internaltypes.ResourceList
	// Used for in queue ordering
	cost                float64
	costToPreempt       float64
	scheduledAtPriority int32
	age                 int64
	// Used for global ordering
	queueCostAfterPreemption     float64
	costAsPercentageOfQueueShare float64
	queuePreemptedOrdinal        int
}

type internalQueueOrder []*runEvictionInfo

func (iqo internalQueueOrder) Len() int {
	return len(iqo)
}

func (iqo internalQueueOrder) Less(i, j int) bool {
	if iqo[i].costToPreempt < iqo[j].costToPreempt {
		return true
	}
	if iqo[i].costToPreempt == iqo[j].costToPreempt {
		// If the cost to preempt is the same for both, preempt the one scheduled at the lower priority first
		if iqo[i].scheduledAtPriority != iqo[j].scheduledAtPriority {
			return iqo[i].scheduledAtPriority < iqo[j].scheduledAtPriority
		}
		if iqo[i].cost != iqo[j].cost {
			return iqo[i].cost < iqo[j].cost
		}
		if iqo[i].age != iqo[j].age {
			return iqo[i].age < iqo[j].age
		}
		return iqo[i].jobId < iqo[j].jobId
	}

	return false
}

func (iqo internalQueueOrder) Swap(i, j int) {
	iqo[i], iqo[j] = iqo[j], iqo[i]
}

type globalPreemptionOrder []*runEvictionInfo

func (gpo globalPreemptionOrder) Len() int {
	return len(gpo)
}

func (gpo globalPreemptionOrder) Less(i, j int) bool {
	if gpo[i].queue == gpo[j].queue {
		return gpo[i].queuePreemptedOrdinal < gpo[j].queuePreemptedOrdinal
	}

	if gpo[i].queueCostAfterPreemption < gpo[j].queueCostAfterPreemption {
		return true

	}
	if gpo[i].queueCostAfterPreemption == gpo[j].queueCostAfterPreemption {
		if gpo[i].costAsPercentageOfQueueShare != gpo[j].costAsPercentageOfQueueShare {
			return gpo[i].costAsPercentageOfQueueShare < gpo[j].costAsPercentageOfQueueShare
		}
		if gpo[i].age != gpo[j].age {
			return gpo[i].age < gpo[j].age
		}
		return gpo[i].jobId < gpo[j].jobId
	}

	return false
}

func (gpo globalPreemptionOrder) Swap(i, j int) {
	gpo[i], gpo[j] = gpo[j], gpo[i]
}

type schedulingResult struct {
	scheduled      bool
	reason         string
	schedulingCost float64
	results        []*nodeSchedulingResult
}

type nodeSchedulingResult struct {
	scheduled bool
	jctx      *context.JobSchedulingContext
	node      *internaltypes.Node
	info      *nodeSchedulingInfo
}

type nodeSchedulingInfo struct {
	schedulingCost     float64
	jobIdsToPreempt    []string
	maximumQueueImpact float64
	queueCostChanges   map[string]float64
	// Used to tie-break when sorting
	resultId string
}

type nodeCostOrder []*nodeSchedulingResult

func (nco nodeCostOrder) Len() int {
	return len(nco)
}

func (nco nodeCostOrder) Less(i, j int) bool {
	if nco[i].info.schedulingCost < nco[j].info.schedulingCost {
		return true
	}
	if nco[i].info.schedulingCost == nco[j].info.schedulingCost {
		if nco[i].info.maximumQueueImpact != nco[j].info.maximumQueueImpact {
			return nco[i].info.maximumQueueImpact < nco[j].info.maximumQueueImpact
		}

		return nco[i].info.resultId < nco[j].info.resultId
	}

	return false
}

func (nco nodeCostOrder) Swap(i, j int) {
	nco[i], nco[j] = nco[j], nco[i]
}

func (n *NodeScheduler) calculateCostToScheduleOnNode(schedContext *domain.SchedulingContext, jctx *context.JobSchedulingContext, node *internaltypes.Node) (*nodeSchedulingResult, error) {

	queues := map[string][]*runEvictionInfo{}
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

		runInfo := &runEvictionInfo{
			cost:                cost,
			resources:           resource,
			jobId:               jobId,
			queue:               queue,
			scheduledAtPriority: scheduledAtPriority,
			age:                 age,
		}

		if _, present := queues[queue]; !present {
			queues[queue] = []*runEvictionInfo{runInfo}
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

	allJobs := []*runEvictionInfo{}

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
		scheduled: true,
		node:      node,
		info: &nodeSchedulingInfo{
			schedulingCost:   totalCost,
			jobIdsToPreempt:  jobsToPreempt,
			queueCostChanges: queueCostChanges,
			resultId:         util.NewULID(),
		},
	}, nil
}
