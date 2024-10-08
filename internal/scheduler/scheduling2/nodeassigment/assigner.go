package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

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

func (a *NodeAssigner) AssignNodesForGang(gang *context.GangSchedulingContext, evictedJobs []*context.JobSchedulingContext) (model.GangAssignmentResult, error) {
	txn := a.nodeDb.Txn()
	defer txn.RollBack()

	assignmentResults := make([]model.AssigmentResult, len(gang.JobSchedulingContexts))
	for i, jctx := range gang.JobSchedulingContexts {

		// Try scheduling job on home nodes
		result, err := a.tryAssignToHomeNodes(jctx, txn, evictedJobs)
		if err != nil {
			return model.GangAssignmentResult{}, err
		}

		// If home scheduling failed, try away nodes
		if !result.Scheduled {
			result, err = a.tryAssignToAwayNodesNodes(jctx, txn, evictedJobs)
			if err != nil {
				return model.GangAssignmentResult{}, err
			}
			if !result.Scheduled {
				return model.GangAssignmentResult{}, nil
			}
		}

		txn.AssignJobToNode(jctx.Job, result.NodeId, result.Priority)
		assignmentResults[i] = result
	}

	txn.Commit()
	return model.GangAssignmentResult{Scheduled: true, JobAssignmentResults: assignmentResults}, nil
}

func (a *NodeAssigner) tryAssignToHomeNodes(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, evictedJobs []*context.JobSchedulingContext) (model.AssigmentResult, error) {
	return a.assignNodeForJob(jctx, txn, evictedJobs)
}

func (a *NodeAssigner) tryAssignToAwayNodesNodes(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, evictedJobs []*context.JobSchedulingContext) (model.AssigmentResult, error) {
	return model.AssigmentResult{}, nil
}

func (a *NodeAssigner) assignNodeForJob(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction, evictedJobs []*context.JobSchedulingContext) (model.AssigmentResult, error) {
	for _, strategy := range a.strategies {
		result, err := strategy(jctx, txn, evictedJobs)
		if err != nil {
			return model.AssigmentResult{}, err
		}
		if result.Scheduled {
			return result, nil
		}
	}
	return model.AssigmentResult{Scheduled: false}, nil
}
