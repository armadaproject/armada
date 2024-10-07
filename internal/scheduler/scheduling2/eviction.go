package scheduling2

import (
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type FairShareEvictor struct{}

func (e *FairShareEvictor) Evict([]*context.JobSchedulingContext) map[string]*context.JobSchedulingContext {
	return nil
}

type OversubscribedEvictor struct{}

func (e *OversubscribedEvictor) Evict([]*context.JobSchedulingContext) map[string]*context.JobSchedulingContext {
	return nil
}
