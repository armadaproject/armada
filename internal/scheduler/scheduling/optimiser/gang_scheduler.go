package optimiser

import (
	"fmt"
	"sort"
	"strings"
	"time"

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

type FairnessOptimisingGangScheduler struct {
	nodeScheduler                    NodeScheduler
	jobDb                            jobdb.JobRepository
	nodeDb                           *nodedb.NodeDb
	minFairnessImprovementPercentage float64
}

func NewFairnessOptimisingScheduler(
	nodeScheduler NodeScheduler,
	jobDb jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
	minFairnessImprovementPercentage float64,
) *FairnessOptimisingGangScheduler {
	return &FairnessOptimisingGangScheduler{
		nodeScheduler:                    nodeScheduler,
		nodeDb:                           nodeDb,
		jobDb:                            jobDb,
		minFairnessImprovementPercentage: minFairnessImprovementPercentage,
	}
}

func (n *FairnessOptimisingGangScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error) {
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
		// TODO differentiate between no options and no options due to lack of fairness improvement
		reason := constraints.JobDoesNotFitUnschedulableReason
		if gctx.Cardinality() > 1 {
			reason = constraints.GangDoesNotFitUnschedulableReason
		}
		return false, nil, reason, nil
	}

	sort.Sort(schedulingCostOrder(schedulingCandidates))
	selectedCandidate := schedulingCandidates[0]
	allPreemptedJobs, err := n.markJobsScheduledAndPreempted(selectedCandidate, sctx)
	if err != nil {
		return false, nil, "", err
	}
	n.logSchedulingResult(ctx, gctx, sctx, selectedCandidate)
	return true, allPreemptedJobs, "", nil
}

func (n *FairnessOptimisingGangScheduler) scheduleOnNodes(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, nodes []*internaltypes.Node) (*schedulingResult, error) {
	combinedResult := &schedulingResult{
		scheduled:      true,
		schedulingCost: 0,
		results:        make([]*nodeSchedulingResult, 0, len(gctx.JobSchedulingContexts)),
	}

	schedulingContext := FromSchedulingContext(sctx)

	for _, jctx := range gctx.JobSchedulingContexts {
		candidateNodes := make([]*nodeSchedulingResult, 0, len(nodes))
		cost := sctx.FairnessCostProvider.UnweightedCostFromAllocation(jctx.Job.AllResourceRequirements())
		for _, node := range nodes {
			result, err := n.nodeScheduler.Schedule(schedulingContext, jctx, node)
			if err != nil {
				return nil, err
			}

			if !result.scheduled {
				continue
			}

			if result.schedulingCost == 0 {
				candidateNodes = append(candidateNodes, result)
				break
			}

			improvementPercentage := ((cost / result.schedulingCost) * 100) - 100
			if improvementPercentage > n.minFairnessImprovementPercentage {
				candidateNodes = append(candidateNodes, result)
			}
		}

		if len(candidateNodes) == 0 {
			return &schedulingResult{scheduled: false}, nil
		}

		sort.Sort(nodeCostOrder(candidateNodes))
		selectedCandidate := candidateNodes[0]
		combinedResult.results = append(combinedResult.results, selectedCandidate)
		combinedResult.schedulingCost += selectedCandidate.schedulingCost

		// update node scheduled on + scheduling context, to reflect current jctx being scheduled
		err := n.updateState(selectedCandidate, nodes, schedulingContext)
		if err != nil {
			return nil, err
		}
	}
	return combinedResult, nil
}

func (n *FairnessOptimisingGangScheduler) updateState(result *nodeSchedulingResult, nodes []*internaltypes.Node, schedulingContext *SchedulingContext) error {
	updatedNode := result.node.DeepCopyNilKeys()
	jobsToPreempt := make([]*jobdb.Job, 0, len(result.jobIdsToPreempt))

	for _, jobId := range result.jobIdsToPreempt {
		job := n.jobDb.GetById(jobId)
		if job == nil {
			return fmt.Errorf("failed to find job %s in job db", jobId)
		}
		jobsToPreempt = append(jobsToPreempt, job)
	}

	updatedNode, err := n.nodeDb.UnbindJobsFromNode(jobsToPreempt, updatedNode)
	if err != nil {
		return err
	}
	updatedNode, err = n.nodeDb.BindJobToNode(updatedNode, result.jctx.Job, result.jctx.Job.PriorityClass().Priority)
	if err != nil {
		return err
	}

	for i, n := range nodes {
		if n.GetId() == updatedNode.GetId() {
			nodes[i] = updatedNode
			break
		}
	}
	for queueName, costChange := range result.queueCostChanges {
		queue, ok := schedulingContext.Queues[queueName]
		if !ok {
			return fmt.Errorf("failed to find queue context for queue %s", queueName)
		}
		queue.CurrentCost += costChange
	}
	return nil
}

func (n *FairnessOptimisingGangScheduler) markJobsScheduledAndPreempted(result *schedulingResult, sctx *context.SchedulingContext) ([]*context.JobSchedulingContext, error) {
	txn := n.nodeDb.Txn(true)
	defer txn.Abort()
	allPreemptedJobs := []*context.JobSchedulingContext{}
	for _, result := range result.results {
		jctx := result.jctx
		jobsToPreempt := make([]*jobdb.Job, 0, len(result.jobIdsToPreempt))

		for _, jobId := range result.jobIdsToPreempt {
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

		err = n.nodeDb.UpsertWithTxn(txn, node)
		if err != nil {
			return nil, err
		}

		preemptedJobs := make([]*context.JobSchedulingContext, 0, len(jobsToPreempt))
		for _, jobToPreempt := range jobsToPreempt {
			preemptedJctx := context.JobSchedulingContextFromJob(jobToPreempt)
			preemptedJctx.PreemptingJobId = preemptedJctx.JobId
			preemptedJctx.PreemptionDescription = fmt.Sprintf("Preempted by scheduler using fairness optimiser - preempting job %s", jctx.JobId)
			preemptedJobs = append(preemptedJobs, preemptedJctx)

			_, err = sctx.PreemptJob(preemptedJctx)
			if err != nil {
				return nil, err
			}
		}
		allPreemptedJobs = append(allPreemptedJobs, preemptedJobs...)

		pctx := &context.PodSchedulingContext{
			Created:             time.Now(),
			ScheduledAtPriority: jctx.Job.PriorityClass().Priority,
			PreemptedAtPriority: internaltypes.MinPriority,
			NumNodes:            n.nodeDb.NumNodes(),
			NodeId:              node.GetId(),
			SchedulingMethod:    context.ScheduledWithFairnessOptimiser,
		}
		jctx.PodSchedulingContext = pctx
		jctx.UnschedulableReason = ""
		_, err = sctx.AddJobSchedulingContext(jctx)
		if err != nil {
			return nil, err
		}
	}
	txn.Commit()
	return allPreemptedJobs, nil
}

func (n *FairnessOptimisingGangScheduler) logSchedulingResult(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, result *schedulingResult) {
	if result == nil {
		return
	}

	cost := result.schedulingCost
	gain := sctx.FairnessCostProvider.UnweightedCostFromAllocation(gctx.TotalResourceRequests)
	improvement := cost / gain

	preemptedJobIds := []string{}
	for _, jobResult := range result.results {
		preemptedJobIds = append(preemptedJobIds, jobResult.jobIdsToPreempt...)
	}

	ctx.Infof("scheduled gctx with job ids %s, costing %f but gaining %f for a fairness improvement of %f, preempting %s",
		strings.Join(gctx.JobIds(), ","), cost, gain, improvement, strings.Join(preemptedJobIds, ","))
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
