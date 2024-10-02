package nodeassigment

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
	"github.com/armadaproject/armada/internal/scheduler/scheduling2/model"
)

type UrgencyNodeAssigner struct {
	nodeDb model.NodeDb
}

func (a *UrgencyNodeAssigner) AssignNode(gang *context.GangSchedulingContext) (model.AssigmentResult, error) {
	return model.AssigmentResult{}, nil
}
