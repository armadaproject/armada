package optimiser

import (
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/scheduling/context"
)

type schedulingResult struct {
	scheduled      bool
	schedulingCost float64
	schedulingGain float64
	nodeResults    []*nodeSchedulingResult
	// Used to tie-break when sorting
	resultId string
}

// We could make this net gain order, but in practice gain is equivalent across all candidates
//   - net_gain = gain - cost
//   - gain varies across schedulingResults but cost does not, so optimising for minimum cost is equivalent to
//     optimising for maximum net gain
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

// node is a snapshot from evaluation. Read the transaction before updating it,
// since other placements may have preempted siblings on that node.
type nodeSchedulingResult struct {
	scheduled      bool
	jctx           *context.JobSchedulingContext
	node           *internaltypes.Node
	schedulingCost float64
	// After preemption-reduction on schedulingResult, there's no guarantee that any gangsToPreempt have a sibling on node.
	gangsToPreempt     []*preemptibleGang
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
