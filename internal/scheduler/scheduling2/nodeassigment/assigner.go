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

func (a *NodeAssigner) AssignNodesForGang(gang *context.GangSchedulingContext, evictedJobs []*context.JobSchedulingContext) error {
	txn := a.nodeDb.Txn()
	defer txn.RollBack()

	for _, jctx := range gang.JobSchedulingContexts {
		for _, strategy := range a.strategies {
			result, err := strategy(jctx, txn, evictedJobs)
			if err != nil {
				return err
			}
			if !result.Scheduled {
				return nil
			}
			txn.AssignJobToNode(jctx.Job, result.NodeId, result.Priority)
			break
		}
	}
	txn.Commit()
	return nil
}
