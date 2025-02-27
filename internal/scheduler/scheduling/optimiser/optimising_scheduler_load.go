package optimiser

import (
	"fmt"
	"sort"
	"time"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/jobdb"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type FairnessOptimisingScheduler struct {
	nodeScheduler                *NodeScheduler
	jobDb                        jobdb.JobRepository
	nodeDb                       *nodedb.NodeDb
	factory                      *internaltypes.ResourceListFactory
	fairnessImprovementThreshold float64
}

func NewFairnessOptimisingScheduler(
	nodeScheduler *NodeScheduler,
	jobDb jobdb.JobRepository,
	nodeDb *nodedb.NodeDb,
	fairnessImprovementThreshold float64) *FairnessOptimisingScheduler {
	return &FairnessOptimisingScheduler{
		nodeScheduler:                nodeScheduler,
		nodeDb:                       nodeDb,
		jobDb:                        jobDb,
		fairnessImprovementThreshold: fairnessImprovementThreshold,
	}
}

func (n *FairnessOptimisingScheduler) Schedule(ctx *armadacontext.Context, gctx *context.GangSchedulingContext, sctx *context.SchedulingContext) (bool, []*context.JobSchedulingContext, string, error) {
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
		// TODO use standard reason
		return false, nil, "no optimised scheduling options found", nil
	}

	sort.Sort(schedulingCostOrder(schedulingCandidates))
	allPreemptedJobs, err := n.updateState(schedulingCandidates[0], sctx)
	if err != nil {
		return false, nil, "", err
	}
	return true, allPreemptedJobs, "", nil
}

func (n *FairnessOptimisingScheduler) scheduleOnNodes(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, nodes []*internaltypes.Node) (*schedulingResult, error) {
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
			improvement := cost / result.schedulingCost
			if improvement < n.fairnessImprovementThreshold {
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

		// Update nodes
		updatedNode := selectedCandidate.node.DeepCopyNilKeys()
		jobsToPreempt := make([]*jobdb.Job, 0, len(selectedCandidate.jobIdsToPreempt))

		for _, jobId := range selectedCandidate.jobIdsToPreempt {
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

		for queueName, costChange := range selectedCandidate.queueCostChanges {
			queue, ok := schedulingContext.Queues[queueName]
			if !ok {
				return nil, fmt.Errorf("failed to find queue context for queue %s", queueName)
			}
			queue.CurrentCost += costChange
		}
	}
	return combinedResult, nil
}

func (n *FairnessOptimisingScheduler) updateState(result *schedulingResult, sctx *context.SchedulingContext) ([]*context.JobSchedulingContext, error) {
	// TODO perform in transaction
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

		err = n.nodeDb.Upsert(node)
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
	return allPreemptedJobs, nil
}

func (n *FairnessOptimisingScheduler) groupNodesByNodeUniformityLabel(nodeUniformityLabel string, nodes []*internaltypes.Node) map[string][]*internaltypes.Node {
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

func (n *FairnessOptimisingScheduler) isValidNodeUniformityLabel(nodeUniformityLabel string) (bool, string) {
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
