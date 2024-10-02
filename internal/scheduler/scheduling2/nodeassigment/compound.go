package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type CompoundNodeAssigner struct {
	assigners []model.NodeAssigner
}

func (a *CompoundNodeAssigner) AssignNode(gctx *context.GangSchedulingContext) (model.AssigmentResult, error) {
	for _, na := range a.assigners {
		result, err := na.AssignNode(gctx)
		if err != nil || result.Scheduled {
			return result, err
		}
	}
	return model.AssigmentResult{}, nil
}
