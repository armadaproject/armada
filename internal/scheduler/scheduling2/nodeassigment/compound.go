package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type CompoundNodeAssigner struct {
	assigners []model.NodeAssigner
}

func (a *CompoundNodeAssigner) AssignNode(jctx *context.JobSchedulingContext) (*internaltypes.Node, error) {
	for _, na := range a.assigners {
		node, err := na.AssignNode(jctx)
		if node != nil || err != nil {
			return node, nil
		}
	}
	return nil, nil
}
