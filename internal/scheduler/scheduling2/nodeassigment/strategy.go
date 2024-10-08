package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type nodeStrategy func(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, eligableEvictions []*model.EvictedJob) (model.AssigmentResult, error)

func assignWithoutPreemption(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, _ []*model.EvictedJob) (model.AssigmentResult, error) {
	return assignAtPriority(jctx, txn, -1)
}

func assignWithUrgencyPreemption(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, _ []*model.EvictedJob) (model.AssigmentResult, error) {
	return assignAtPriority(jctx, txn, jctx.Job.PriorityClass().Priority)
}

func assignWithFairSharePreemption(
	jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, eligibleEvictions []*model.EvictedJob) (model.AssigmentResult, error) {

	type consideredNode struct {
		node              *internaltypes.Node
		availableResource internaltypes.ResourceList
		evictedJobs       []*model.EvictedJob
	}

	nodesById := make(map[string]*consideredNode)

	for _, evictedJob := range eligibleEvictions {

		// Resolve the node for the evicted job
		node, ok := nodesById[evictedJob.NodeId]
		if !ok {
			dbNode := txn.GetNodeById(evictedJob.NodeId)
			node = &consideredNode{
				node:              dbNode,
				availableResource: internaltypes.ResourceList{},
				evictedJobs:       []*model.EvictedJob{},
			}
			nodesById[evictedJob.NodeId] = node
		}

		// Remove the evicted job from the node
		node.availableResource = node.availableResource.Add(evictedJob.Job.EfficientResourceRequirements())
		node.evictedJobs = append(node.evictedJobs, evictedJob)

		// See if the new job now fits
		dynamicRequirementsMet, _ := nodedb.DynamicJobRequirementsMet(node.availableResource, jctx)
		staticRequirementsMet, _, err := nodedb.StaticJobRequirementsMet(node.node, jctx)
		if err != nil {
			return model.AssigmentResult{}, err
		}

		// If the job now fits, preempt all the evicted jobs on that node
		if dynamicRequirementsMet && staticRequirementsMet {
			for _, preemptedJob := range node.evictedJobs {
				txn.UnassignJobFromNode(preemptedJob.Job, node.node.GetId())
			}
			return model.AssigmentResult{
				Scheduled: true,
				NodeId:    node.node.GetId(),
			}, nil
		}
	}

	return model.AssigmentResult{}, nil
}

func assignAtPriority(
	jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, priority int32) (model.AssigmentResult, error) {
	node := txn.GetNode(
		priority,
		jctx.Job.EfficientResourceRequirements(),
		jctx.Job.Tolerations(),
		jctx.Job.NodeSelector(),
		jctx.Job.Affinity())

	if node != "" {
		return model.AssigmentResult{
			Scheduled: true,
			Priority:  jctx.Job.PriorityClass().Priority,
			NodeId:    node,
		}, nil
	}
	return model.AssigmentResult{}, nil
}
