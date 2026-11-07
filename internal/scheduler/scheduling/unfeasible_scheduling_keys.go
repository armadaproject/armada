package scheduling

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/constraints"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

func recordUnfeasibleSchedulingKey(gctx *context.GangSchedulingContext, sctx *context.SchedulingContext, unschedulableReason string) {
	globallyUnschedulable := constraints.UnschedulableReasonIsPropertyOfGang(unschedulableReason)
	// A gang may fail even when its members are individually schedulable.
	if gctx.Cardinality() != 1 || !globallyUnschedulable {
		return
	}
	jctx := gctx.JobSchedulingContexts[0]
	schedulingKey, ok := jctx.SchedulingKey()
	if !ok || schedulingKey == internaltypes.EmptySchedulingKey {
		return
	}
	// Keep the exact first context already stored for this key.
	if _, ok := sctx.UnfeasibleSchedulingKeys[schedulingKey]; !ok {
		sctx.UnfeasibleSchedulingKeys[schedulingKey] = jctx
	}
}
