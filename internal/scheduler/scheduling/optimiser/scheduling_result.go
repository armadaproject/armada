package optimiser

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type schedulingResult struct {
	scheduled      bool
	reason         string
	schedulingCost float64
	results        []*nodeSchedulingResult
	// Used to tie-break when sorting
	resultId string
}

type schedulingCostOrder []*schedulingResult

func (sco schedulingCostOrder) Len() int {
	return len(sco)
}

func (sco schedulingCostOrder) Less(i, j int) bool {
	if sco[i].schedulingCost < sco[j].schedulingCost {
		return true
	}
	if sco[i].schedulingCost == sco[j].schedulingCost {
		return sco[i].resultId < sco[j].resultId
	}

	return false
}

func (sco schedulingCostOrder) Swap(i, j int) {
	sco[i], sco[j] = sco[j], sco[i]
}

type nodeSchedulingResult struct {
	scheduled          bool
	jctx               *context.JobSchedulingContext
	node               *internaltypes.Node
	schedulingCost     float64
	jobIdsToPreempt    []string
	maximumQueueImpact float64
	queueCostChanges   map[string]float64
	// Used to tie-break when sorting
	resultId string
}

type nodeCostOrder []*nodeSchedulingResult

func (nco nodeCostOrder) Len() int {
	return len(nco)
}

func (nco nodeCostOrder) Less(i, j int) bool {
	if nco[i].schedulingCost < nco[j].schedulingCost {
		return true
	}
	if nco[i].schedulingCost == nco[j].schedulingCost {
		if nco[i].maximumQueueImpact != nco[j].maximumQueueImpact {
			return nco[i].maximumQueueImpact < nco[j].maximumQueueImpact
		}

		return nco[i].resultId < nco[j].resultId
	}

	return false
}

func (nco nodeCostOrder) Swap(i, j int) {
	nco[i], nco[j] = nco[j], nco[i]
}
