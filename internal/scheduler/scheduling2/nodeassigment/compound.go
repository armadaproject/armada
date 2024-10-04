package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/nodedb"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type nodeStrategy func(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction) (model.AssigmentResult, error)

type NodeAssigner struct {
	nodeDb     model.NodeDb
	strategies []nodeStrategy
}

func NewNodeAssigner(nodeDb model.NodeDb) *NodeAssigner {
	return &NodeAssigner{
		nodeDb: nodeDb,
		strategies: []nodeStrategy{
			assignWithoutPreemption,
			assignWithFairSharePreemption,
			assignWithUrgencyPreemption,
		},
	}
}

func (a *NodeAssigner) AssignNodesForGang(gang *context.GangSchedulingContext) error {
	txn := a.nodeDb.Txn()
	defer txn.RollBack()

	for _, jctx := range gang.JobSchedulingContexts {
		for _, strategy := range a.strategies {
			result, err := strategy(jctx, txn)
			if err != nil {
				return err
			}
			if result.Scheduled {
				txn.AssignJobToNode(jctx.Job, result.NodeId, result.Priority)
				break
			} else {
				return nil
			}
		}
	}
	txn.Commit()
	return nil
}

func assignWithoutPreemption(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction) (model.AssigmentResult, error) {
	nodes := txn.GetNodes(
		-1,
		jctx.Job.EfficientResourceRequirements(),
		jctx.Job.Tolerations(),
		jctx.Job.NodeSelector(),
		jctx.Job.Affinity())

	node := nodes.Next()
	if node != nil {
		txn.AssignJobToNode(jctx.Job, node.GetId(), jctx.Job.PriorityClass().Priority)
		return model.AssigmentResult{
			Scheduled: true,
			NodeId:    node.GetId(),
		}, nil
	}
	return model.AssigmentResult{}, nil
}

func assignWithFairSharePreemption(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction) (model.AssigmentResult, error) {
	type consideredNode struct {
		node              *internaltypes.Node
		availableResource internaltypes.ResourceList
		evictedJobs       []*model.EvictedJob
	}

	nodesById := make(map[string]*consideredNode)

	for _, evictedJob := range a.evictedJobs {

		// Resolve the node for the evicted job
		node, ok := nodesById[evictedJob.NodeId]
		if !ok {
			dbNode := a.nodeDb.GetNodeById(evictedJob.NodeId)
			node = &consideredNode{
				node:              dbNode,
				availableResource: internaltypes.ResourceList{},
				evictedJobs:       []*model.EvictedJob{},
			}
			nodesById[evictedJob.NodeId] = node
		}

		// Remove the evicted job from the node
		node.availableResource = node.availableResource.Add(evictedJob.Resources)
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
				node.node = a.preepmtJob(preemptedJob, node.node)
			}
			return model.AssigmentResult{
				Scheduled: true,
				NodeId:    node.node.GetId(),
			}, nil
		}
	}

	return model.AssigmentResult{}, nil
}

func assignWithUrgencyPreemption(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction) (model.AssigmentResult, error) {
	return model.AssigmentResult{}, nil
}
