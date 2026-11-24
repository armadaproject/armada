package optimiser

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

// A preemptibleGang contains the allocated members of a gang, or a single non-gang job.
type preemptibleGang struct {
	preemptedSiblingJobId string
	queue                 string
	// applicableResources are the only resources applicable to the job being considered for scheduling
	applicableResources internaltypes.ResourceList
	jobs                []*context.JobSchedulingContext
	totalResources      internaltypes.ResourceList
	// lowerPriorityResources are resources of a lower priority than the job being considered for scheduling
	lowerPriorityResources internaltypes.ResourceList
	// Used for in queue ordering
	cost                float64
	costToPreempt       float64
	crossPool           bool
	priorityPreemption  bool
	scheduledAtPriority int32
	ageMillis           int64
	// Used for global ordering
	weightedCostAfterPreemption float64
	queuePreemptedOrdinal       int
}

// unweightedPreemptionCost charges only the increase in shortfall below fair share caused
// by equal-priority members. Lower-priority members can be preempted for free.
func (g *preemptibleGang) unweightedPreemptionCost(sctx *SchedulingContext, allocation internaltypes.ResourceList) float64 {
	fairshare := sctx.Queues[g.queue].Fairshare
	costProvider := sctx.Sctx.FairnessCostProvider
	before := costProvider.UnweightedCostFromAllocation(allocation.Subtract(g.lowerPriorityResources))
	after := costProvider.UnweightedCostFromAllocation(allocation.Subtract(g.totalResources))
	return max(0, fairshare-after) - max(0, fairshare-before)
}

type internalQueueOrder []*preemptibleGang

func (iqo internalQueueOrder) Len() int {
	return len(iqo)
}

func (iqo internalQueueOrder) Less(i, j int) bool {
	if iqo[i].costToPreempt < iqo[j].costToPreempt {
		return true
	}
	if iqo[i].costToPreempt == iqo[j].costToPreempt {
		// If the schedulingCost to preempt is the same for both, preempt the one scheduled at the lower priority first
		if iqo[i].scheduledAtPriority != iqo[j].scheduledAtPriority {
			return iqo[i].scheduledAtPriority < iqo[j].scheduledAtPriority
		}
		if iqo[i].cost != iqo[j].cost {
			return iqo[i].cost < iqo[j].cost
		}
		if iqo[i].ageMillis != iqo[j].ageMillis {
			return iqo[i].ageMillis < iqo[j].ageMillis
		}
		return iqo[i].preemptedSiblingJobId < iqo[j].preemptedSiblingJobId
	}

	return false
}

func (iqo internalQueueOrder) Swap(i, j int) {
	iqo[i], iqo[j] = iqo[j], iqo[i]
}

type globalPreemptionOrder []*preemptibleGang

func (gpo globalPreemptionOrder) Len() int {
	return len(gpo)
}

func (gpo globalPreemptionOrder) Less(i, j int) bool {
	if gpo[i].queue == gpo[j].queue {
		return gpo[i].queuePreemptedOrdinal < gpo[j].queuePreemptedOrdinal
	}

	// Cross pool jobs should be preempted ahead of jobs in the current pool
	if gpo[i].crossPool != gpo[j].crossPool {
		return gpo[i].crossPool
	}

	// Priority preemption is currently known to be unfair
	if gpo[i].priorityPreemption != gpo[j].priorityPreemption {
		return gpo[i].priorityPreemption
	}

	if gpo[i].weightedCostAfterPreemption > gpo[j].weightedCostAfterPreemption {
		return true
	}
	if gpo[i].weightedCostAfterPreemption == gpo[j].weightedCostAfterPreemption {
		if gpo[i].scheduledAtPriority != gpo[j].scheduledAtPriority {
			return gpo[i].scheduledAtPriority < gpo[j].scheduledAtPriority
		}
		if gpo[i].cost != gpo[j].cost {
			return gpo[i].cost < gpo[j].cost
		}
		if gpo[i].ageMillis != gpo[j].ageMillis {
			return gpo[i].ageMillis < gpo[j].ageMillis
		}
		return gpo[i].preemptedSiblingJobId < gpo[j].preemptedSiblingJobId
	}

	return false
}

func (gpo globalPreemptionOrder) Swap(i, j int) {
	gpo[i], gpo[j] = gpo[j], gpo[i]
}
