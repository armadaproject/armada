package optimiser

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

type preemptibleJobDetails struct {
	// metadata
	jobId     string
	queue     string
	resources internaltypes.ResourceList
	// Used for in queue ordering
	cost                float64
	costToPreempt       float64
	priorityPreemption  bool
	scheduledAtPriority int32
	ageMillis           int64
	// Used for global ordering
	weightedCostAfterPreemption float64
	queuePreemptedOrdinal       int
}

type internalQueueOrder []*preemptibleJobDetails

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
		return iqo[i].jobId < iqo[j].jobId
	}

	return false
}

func (iqo internalQueueOrder) Swap(i, j int) {
	iqo[i], iqo[j] = iqo[j], iqo[i]
}

type globalPreemptionOrder []*preemptibleJobDetails

func (gpo globalPreemptionOrder) Len() int {
	return len(gpo)
}

func (gpo globalPreemptionOrder) Less(i, j int) bool {
	if gpo[i].queue == gpo[j].queue {
		return gpo[i].queuePreemptedOrdinal < gpo[j].queuePreemptedOrdinal
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
		return gpo[i].jobId < gpo[j].jobId
	}

	return false
}

func (gpo globalPreemptionOrder) Swap(i, j int) {
	gpo[i], gpo[j] = gpo[j], gpo[i]
}
