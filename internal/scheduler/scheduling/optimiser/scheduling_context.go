package optimiser

import "github.com/armadaproject/armada/internal/scheduler/scheduling/context"

type SchedulingContext struct {
	Sctx   *context.SchedulingContext
	Queues map[string]*QueueContext
}

func FromSchedulingContext(sctx *context.SchedulingContext) *SchedulingContext {
	schedulingContext := &SchedulingContext{
		Sctx:   sctx,
		Queues: make(map[string]*QueueContext, len(sctx.QueueSchedulingContexts)),
	}

	for _, qctx := range sctx.QueueSchedulingContexts {
		queueContext := &QueueContext{
			Name:        qctx.Queue,
			CurrentCost: sctx.FairnessCostProvider.UnweightedCostFromQueue(qctx),
			Fairshare:   qctx.DemandCappedAdjustedFairShare,
			Weight:      qctx.Weight,
		}
		schedulingContext.Queues[queueContext.Name] = queueContext
	}

	return schedulingContext
}

type QueueContext struct {
	Name        string
	CurrentCost float64
	Fairshare   float64
	Weight      float64
}
