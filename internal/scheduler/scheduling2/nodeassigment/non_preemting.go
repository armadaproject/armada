package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type NonPreemptingNodeAssigner struct {
	nodeDb model.NodeDb
}

func (a *NonPreemptingNodeAssigner) AssignNode(gang *context.GangSchedulingContext) (model.AssigmentResult, error) {
	nodes := a.nodeDb.GetNodes(
		-1,
		jctx.Job.EfficientResourceRequirements(),
		jctx.Job.Tolerations(),
		jctx.Job.NodeSelector(),
		jctx.Job.Affinity())

	node := nodes.Next()
	if node != nil {
		// bind job to node
	}
	return model.AssigmentResult{}, nil
}
