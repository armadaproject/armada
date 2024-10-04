package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type NonPreemptingNodeAssigner struct {
	nodeDb model.NodeDb
}

func (a *NonPreemptingNodeAssigner) AssignNodeToJob(jctx *context.JobSchedulingContext, txn model.NodeDbTransaction) (model.AssigmentResult, error) {
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
