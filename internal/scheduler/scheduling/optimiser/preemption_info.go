package optimiser

import "github.com/armadaproject/armada/internal/scheduler/internaltypes"

type runPreemptionInfo struct {
	// metadata
	jobId string
	queue string
	// TODO make pointer
	resources internaltypes.ResourceList
	// Used for in queue ordering
	cost                float64
	costToPreempt       float64
	scheduledAtPriority int32
	age                 int64
	// Used for global ordering
	queueCostAfterPreemption     float64
	costAsPercentageOfQueueShare float64
	queuePreemptedOrdinal        int
}

type internalQueueOrder []*runPreemptionInfo

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
		if iqo[i].age != iqo[j].age {
			return iqo[i].age < iqo[j].age
		}
		return iqo[i].jobId < iqo[j].jobId
	}

	return false
}

func (iqo internalQueueOrder) Swap(i, j int) {
	iqo[i], iqo[j] = iqo[j], iqo[i]
}

type globalPreemptionOrder []*runPreemptionInfo

func (gpo globalPreemptionOrder) Len() int {
	return len(gpo)
}

func (gpo globalPreemptionOrder) Less(i, j int) bool {
	if gpo[i].queue == gpo[j].queue {
		return gpo[i].queuePreemptedOrdinal < gpo[j].queuePreemptedOrdinal
	}

	if gpo[i].queueCostAfterPreemption < gpo[j].queueCostAfterPreemption {
		return true

	}
	if gpo[i].queueCostAfterPreemption == gpo[j].queueCostAfterPreemption {
		if gpo[i].costAsPercentageOfQueueShare != gpo[j].costAsPercentageOfQueueShare {
			return gpo[i].costAsPercentageOfQueueShare < gpo[j].costAsPercentageOfQueueShare
		}
		if gpo[i].age != gpo[j].age {
			return gpo[i].age < gpo[j].age
		}
		return gpo[i].jobId < gpo[j].jobId
	}

	return false
}

func (gpo globalPreemptionOrder) Swap(i, j int) {
	gpo[i], gpo[j] = gpo[j], gpo[i]
}
