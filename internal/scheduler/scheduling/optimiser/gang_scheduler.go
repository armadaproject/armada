package optimiser

import (
	stdcontext "context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/hashicorp/go-memdb"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type GangScheduler interface {
	Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error)
}

// FairnessOptimisingGangScheduler The optimiser's objective is to minimise cumulative absolute fairness error.
//   - Fairness error is defined as the positive difference in cost between a user's demand capped adjusted fair share and their allocation.
//   - A limitation of this approach is that it doesn't consider users who are slightly below their DCAFS and well below their AFS. They could game the optimiser by submitting dummy workloads to increase the gap outlined in the objective.
//   - We accept this limitation since the core scheduling loop is likely to resolve these cases directly.
//   - Low priority resources are excluded from fairness error accounting, since high priority jobs strictly supercede low priority jobs under fair share scheduling.
type FairnessOptimisingGangScheduler struct {
	nodeScheduler                    NodeScheduler
	jobDb                            jobdb.JobRepository
	nodeDb                           *nodedb.NodeDb
	minFairnessImprovementPercentage float64
	allowGangPreemption              bool
}

func NewFairnessOptimisingScheduler(
	nodeScheduler NodeScheduler,
	jobDb jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
	minFairnessImprovementPercentage float64,
	allowGangPreemption bool,
) *FairnessOptimisingGangScheduler {
	return &FairnessOptimisingGangScheduler{
		nodeScheduler:                    nodeScheduler,
		nodeDb:                           nodeDb,
		jobDb:                            jobDb,
		minFairnessImprovementPercentage: minFairnessImprovementPercentage,
		allowGangPreemption:              allowGangPreemption,
	}
}

// Schedule
// It will group nodes by the nodeUniformity of the gang
// Attempt to schedule against each group of nodes independently and then pick the best result
func (n *FairnessOptimisingGangScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error) {
	if ctx.Err() != nil {
		return false, nil, "", nil
	}
	nodes, err := n.nodeDb.GetNodes()
	if err != nil {
		return false, nil, "", err
	}

	if isValid, reason := n.isValidNodeUniformityLabel(gctx.NodeUniformityLabel()); !isValid {
		return false, nil, reason, nil
	}

	nodesByNodeUniformityLabel := n.groupNodesByNodeUniformityLabel(gctx.NodeUniformityLabel(), nodes)
	schedulingCandidates := make([]*schedulingResult, 0, len(nodesByNodeUniformityLabel))
	unschedulableReason := constraints.OptimiserNoPlacementUnschedulableReason

	for group, groupedNodes := range nodesByNodeUniformityLabel {
		if ctx.Err() != nil {
			break
		}
		result, reason, err := n.scheduleOnNodes(ctx, gctx, sctx, groupedNodes)
		if err != nil || result == nil {
			return false, nil, "", err
		}
		if result.scheduled {
			result.resultId = group
			schedulingCandidates = append(schedulingCandidates, result)
		}
		if reason != "" {
			unschedulableReason = reason
		}
	}

	if ctx.Err() == stdcontext.Canceled || (ctx.Err() != nil && len(schedulingCandidates) == 0) {
		return false, nil, "", nil
	}
	if len(schedulingCandidates) == 0 {
		return false, nil, unschedulableReason, nil
	}

	sort.Sort(schedulingCostOrder(schedulingCandidates))
	selectedCandidate := schedulingCandidates[0]
	allPreemptedJobs, err := n.markJobsScheduledAndPreempted(selectedCandidate, gctx, sctx)
	if err != nil {
		return false, nil, "", err
	}
	n.logSchedulingResult(ctx, gctx, selectedCandidate)
	return true, allPreemptedJobs, "", nil
}

// scheduleOnNodes
// The function attempts to schedule a gang onto the nodes provided
// It uses the nodeScheduler to score each node and picks the best one that exceeds the minFairnessImprovementPercentage
// - If no jobs exceed the minFairnessImprovementPercentage, no scheduling is performed
// - With gang preemption enabled, this threshold applies to the whole gang, since preempting siblings may make later placements free
// The function maintains an internal state that is used if the gang contains more than one job
// - After each time a job is "scheduled" the internal state is updated to reflect this scheduling
// - This is so an updated state can be passed to the nodeScheduler so it can make better decisions
//   - Scheduling a job will likely preempt some jobs, including siblings outside the provided nodes.
//   - The queues with preempted jobs will therefore have their current share reduced, so should be less likely to be preempted for the next job scheduled
func (n *FairnessOptimisingGangScheduler) scheduleOnNodes(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, nodes []*internaltypes.Node) (*schedulingResult, string, error) {
	txn := n.nodeDb.Txn(true)
	defer txn.Abort()
	combinedResult := &schedulingResult{
		scheduled:      true,
		schedulingGain: sctx.FairnessCostProvider.UnweightedCostFromAllocation(gctx.TotalResourceRequests),
		nodeResults:    make([]*nodeSchedulingResult, 0, len(gctx.JobSchedulingContexts)),
	}
	schedulingContext := FromSchedulingContext(sctx)
	schedulingContext.NodeDbTxn = txn

	for _, jctx := range gctx.JobSchedulingContexts {
		candidateNodes := make([]*nodeSchedulingResult, 0, len(nodes))
		unschedulableReason := constraints.OptimiserNoPlacementUnschedulableReason
		cost := sctx.FairnessCostProvider.UnweightedCostFromAllocation(jctx.Job.AllResourceRequirements())
		for _, originalNode := range nodes {
			if ctx.Err() != nil {
				return nil, "", nil
			}
			node, err := n.nodeDb.GetNodeWithTxn(txn, originalNode.GetId())
			if err != nil {
				return nil, "", err
			}
			result, err := n.nodeScheduler.Schedule(schedulingContext, jctx, node)
			if err != nil {
				return nil, "", err
			}
			if !result.scheduled {
				continue
			}

			// No preemption required - this is the ideal result - exit early
			if len(result.gangsToPreempt) == 0 {
				candidateNodes = []*nodeSchedulingResult{result}
				break
			}
			improvementPercentage := ((cost / result.schedulingCost) * 100) - 100
			if n.allowGangPreemption || improvementPercentage > n.minFairnessImprovementPercentage {
				candidateNodes = append(candidateNodes, result)
			} else {
				unschedulableReason = constraints.OptimiserNoFairnessImprovementUnschedulableReason
			}
		}
		if len(candidateNodes) == 0 {
			return &schedulingResult{}, unschedulableReason, nil
		}

		sort.Sort(nodeCostOrder(candidateNodes))
		selectedCandidate := candidateNodes[0]
		combinedResult.nodeResults = append(combinedResult.nodeResults, selectedCandidate)
		combinedResult.schedulingCost += selectedCandidate.schedulingCost
		if err := n.updateState(selectedCandidate, schedulingContext); err != nil {
			return nil, "", err
		}
	}

	if n.allowGangPreemption {
		// Each preemption-reduction trial starts from the original allocation.
		txn.Abort()
		var err error
		combinedResult, err = n.reducePreemptions(ctx, nodes, combinedResult)
		if err != nil {
			return nil, "", err
		}
		if ctx.Err() != nil || combinedResult == nil {
			return nil, "", nil
		}
		cost, gain, err := n.schedulingCostAndGain(gctx, sctx, combinedResult)
		if err != nil {
			return nil, "", err
		}
		combinedResult.schedulingCost = cost
		combinedResult.schedulingGain = gain
		combinedResult.scheduled = gain > cost*(1+n.minFairnessImprovementPercentage/100)
		if !combinedResult.scheduled {
			return combinedResult, constraints.OptimiserNoFairnessImprovementUnschedulableReason, nil
		}
	}
	return combinedResult, "", nil
}

func (n *FairnessOptimisingGangScheduler) updateState(result *nodeSchedulingResult, schedulingContext *SchedulingContext) error {
	for _, gang := range result.gangsToPreempt {
		if err := n.unbindGangWithTxn(schedulingContext.NodeDbTxn, gang); err != nil {
			return err
		}
		queue, ok := schedulingContext.Queues[gang.queue]
		if !ok {
			return fmt.Errorf("failed to find queue context for queue %s", gang.queue)
		}
		queue.AllocationInclShortJobPenalty = queue.AllocationInclShortJobPenalty.Subtract(gang.totalResources)
	}
	for queueName, costChange := range result.queueCostChanges {
		queue, ok := schedulingContext.Queues[queueName]
		if !ok {
			return fmt.Errorf("failed to find queue context for queue %s", queueName)
		}
		queue.CurrentCost += costChange
	}
	return n.bindJobWithTxn(schedulingContext.NodeDbTxn, result.jctx, result.node.GetId())
}

func (n *FairnessOptimisingGangScheduler) unbindGangWithTxn(txn *memdb.Txn, gang *preemptibleGang) error {
	for _, jctx := range gang.jobs {
		node, err := n.nodeDb.GetNodeWithTxn(txn, jctx.AssignedNode.GetId())
		if err != nil {
			return err
		}
		node, err = n.nodeDb.UnbindJobFromNode(jctx.Job, node)
		if err != nil {
			return err
		}
		if err := n.nodeDb.UpsertWithTxn(txn, node); err != nil {
			return err
		}
	}
	return nil
}

func (n *FairnessOptimisingGangScheduler) bindJobWithTxn(txn *memdb.Txn, jctx *context.JobSchedulingContext, nodeId string) error {
	node, err := n.nodeDb.GetNodeWithTxn(txn, nodeId)
	if err != nil {
		return err
	}
	node, err = n.nodeDb.BindJobToNode(node, jctx.Job, jctx.Job.PriorityClass().Priority)
	if err != nil {
		return err
	}
	return n.nodeDb.UpsertWithTxn(txn, node)
}

func (n *FairnessOptimisingGangScheduler) markJobsScheduledAndPreempted(result *schedulingResult, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) ([]*context.JobSchedulingContext, error) {
	txn := n.nodeDb.Txn(true)
	defer txn.Abort()

	allPreemptedJobs := []*context.JobSchedulingContext{}
	for _, placement := range result.nodeResults {
		for _, gang := range placement.gangsToPreempt {
			allJobsScheduledInThisRound := true
			if err := n.unbindGangWithTxn(txn, gang); err != nil {
				return nil, err
			}
			for _, jctx := range gang.jobs {
				jctx.PreemptionDetails = &context.PreemptionDetails{PreemptingJob: placement.jctx.Job}
				if jctx.JobId != gang.preemptedSiblingJobId {
					jctx.PreemptionDetails.PreemptedSiblingJob = n.jobDb.GetById(gang.preemptedSiblingJobId)
				}
				jctx.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using fairness optimiser - preempting job %s", placement.jctx.JobId)
				jctx.PreemptionType = context.PreemptedWithOptimiserPreemption
				scheduledInThisRound, err := sctx.PreemptJob(jctx)
				if err != nil {
					return nil, err
				}
				allJobsScheduledInThisRound = allJobsScheduledInThisRound && scheduledInThisRound
				sctx.MarkJobPreempted(jctx.JobId)
				allPreemptedJobs = append(allPreemptedJobs, jctx)
			}
			// PreemptJob updates job and resource accounting; undo the gang count once for the whole gang.
			if allJobsScheduledInThisRound {
				sctx.NumScheduledGangs--
			}
		}
		placement.jctx.PodSchedulingContext = &context.PodSchedulingContext{
			Created:             time.Now(),
			ScheduledAtPriority: placement.jctx.Job.PriorityClass().Priority,
			PreemptedAtPriority: internaltypes.MinPriority,
			NumNodes:            n.nodeDb.NumNodes(),
			NodeId:              placement.node.GetId(),
			SchedulingMethod:    context.ScheduledWithFairnessOptimiser,
		}
		placement.jctx.UnschedulableReason = ""

		if err := n.bindJobWithTxn(txn, placement.jctx, placement.node.GetId()); err != nil {
			return nil, err
		}
	}
	if _, err := sctx.AddGangSchedulingContext(gctx); err != nil {
		return nil, err
	}
	txn.Commit()
	return allPreemptedJobs, nil
}

// schedulingCostAndGain outputs (cost, gain, error):
//   - cost is the cumulative increase in fairness error across all preempted queues
//   - gain is the decrease in fairness error for the queue of the scheduled gang
//   - Fairness error is defined as the positive difference in cost between a user's demand capped adjusted fair share and their allocation.
func (n *FairnessOptimisingGangScheduler) schedulingCostAndGain(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, result *schedulingResult) (float64, float64, error) {
	schedulingContext := FromSchedulingContext(sctx)
	resourceLostByQueue := map[string]internaltypes.ResourceList{}
	lowPriorityResourcesLostByQueue := map[string]internaltypes.ResourceList{}
	for _, placement := range result.nodeResults {
		for _, gang := range placement.gangsToPreempt {
			qctx := schedulingContext.Queues[gang.queue]
			if qctx == nil {
				return 0, 0, fmt.Errorf("failed to find queue context for queue %s", gang.queue)
			}
			if _, ok := resourceLostByQueue[qctx.Name]; !ok {
				resourceLostByQueue[qctx.Name] = internaltypes.ResourceList{}
				lowPriorityResourcesLostByQueue[qctx.Name] = internaltypes.ResourceList{}
			}
			resourceLostByQueue[qctx.Name] = resourceLostByQueue[qctx.Name].Add(gang.totalResources).Subtract(gang.lowerPriorityResources)
			lowPriorityResourcesLostByQueue[qctx.Name] = lowPriorityResourcesLostByQueue[qctx.Name].Add(gang.lowerPriorityResources)
		}
	}

	cost := float64(0)
	for queue, resourcesLostAtIncomingGangPriority := range resourceLostByQueue {
		qctx := schedulingContext.Queues[queue]
		if qctx == nil {
			return 0, 0, fmt.Errorf("failed to find queue context for queue %s", queue)
		}
		if qctx.Fairshare == 0 {
			continue
		}
		// We deliberately exclude impacts to queue costs from low priority resources, since this
		//  aligns with Fair Share scheduling behaviour
		relevantAllocation := qctx.AllocationInclShortJobPenalty.Subtract(lowPriorityResourcesLostByQueue[queue])
		before := sctx.FairnessCostProvider.UnweightedCostFromAllocation(relevantAllocation)
		after := sctx.FairnessCostProvider.UnweightedCostFromAllocation(relevantAllocation.Subtract(resourcesLostAtIncomingGangPriority))
		// Accumulate the increase in absolute fairness error for all preempted queues
		cost += max(0, qctx.Fairshare-after) - max(0, qctx.Fairshare-before)
	}

	qctx := schedulingContext.Queues[gctx.Queue]
	if qctx == nil {
		return 0, 0, fmt.Errorf("failed to find queue context for gang %s", gctx.Queue)
	}

	before := sctx.FairnessCostProvider.UnweightedCostFromAllocation(qctx.AllocationInclShortJobPenalty)
	after := sctx.FairnessCostProvider.UnweightedCostFromAllocation(qctx.AllocationInclShortJobPenalty.Add(gctx.TotalResourceRequests))

	// Gain is the queue cost improvement for the incoming gang. Gain is the within fair share gain.
	gain := max(0, qctx.Fairshare-before) - max(0, qctx.Fairshare-after)
	return cost, gain, nil
}

func (n *FairnessOptimisingGangScheduler) logSchedulingResult(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, result *schedulingResult) {
	if result == nil {
		return
	}

	cost := result.schedulingCost
	gain := result.schedulingGain
	improvement := gain - cost
	improvementPercentage := (gain / cost * 100) - 100
	preemptedJobIds := []string{}
	for _, jobResult := range result.nodeResults {
		for _, gang := range jobResult.gangsToPreempt {
			for _, jctx := range gang.jobs {
				preemptedJobIds = append(preemptedJobIds, jctx.JobId)
			}
		}
	}

	ctx.Infof("scheduled gctx with job ids %s, costing %f but gaining %f for a fairness improvement of %f (%f%%), preempting %s",
		strings.Join(gctx.JobIds(), ","), cost, gain, improvement, improvementPercentage, strings.Join(preemptedJobIds, ","))
}

func (n *FairnessOptimisingGangScheduler) groupNodesByNodeUniformityLabel(nodeUniformityLabel string, nodes []*internaltypes.Node) map[string][]*internaltypes.Node {
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

func (n *FairnessOptimisingGangScheduler) isValidNodeUniformityLabel(nodeUniformityLabel string) (bool, string) {
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
