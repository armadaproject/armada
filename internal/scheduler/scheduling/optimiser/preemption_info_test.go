package optimiser

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/testfixtures"
	"github.com/armadaproject/armada/pkg/api"
)

func TestPreemptibleGang_PreemptionCost(t *testing.T) {
	// CPU and memory both total 100, so the larger allocation determines cost.
	sctx := FromSchedulingContext(setUpSctx(t, []*api.Queue{queueA, queueB}, nil, testfixtures.CpuMem("100", "100Gi")))
	sctx.Queues["B"].Fairshare = 0.5
	tests := map[string]struct {
		allocation             internaltypes.ResourceList
		resources              internaltypes.ResourceList
		lowerPriorityResources internaltypes.ResourceList
		expectedCost           float64
	}{
		"below fair share": {
			allocation: testfixtures.CpuMem("40", "0"), resources: testfixtures.CpuMem("20", "0"), expectedCost: 0.2,
		},
		"stays above fair share": {
			allocation: testfixtures.CpuMem("80", "0"), resources: testfixtures.CpuMem("20", "0"), expectedCost: 0,
		},
		"crosses fair share": {
			allocation: testfixtures.CpuMem("60", "0"), resources: testfixtures.CpuMem("20", "0"), expectedCost: 0.1,
		},
		"lower-priority members are free": {
			allocation: testfixtures.CpuMem("40", "0"), resources: testfixtures.CpuMem("20", "0"),
			lowerPriorityResources: testfixtures.CpuMem("20", "0"), expectedCost: 0,
		},
		"only equal-priority members are charged": {
			allocation: testfixtures.CpuMem("40", "0"), resources: testfixtures.CpuMem("20", "0"),
			lowerPriorityResources: testfixtures.CpuMem("10", "0"), expectedCost: 0.1,
		},
		"removing CPU leaves memory share unchanged": {
			allocation: testfixtures.CpuMem("20", "40Gi"), resources: testfixtures.CpuMem("20", "0"), expectedCost: 0,
		},
		"removing CPU makes memory dominant": {
			allocation: testfixtures.CpuMem("40", "30Gi"), resources: testfixtures.CpuMem("20", "0"), expectedCost: 0.1,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			gang := &preemptibleGang{queue: "B", totalResources: tc.resources, lowerPriorityResources: tc.lowerPriorityResources}
			assert.InDelta(t, tc.expectedCost, gang.unweightedPreemptionCost(sctx, tc.allocation), 1e-8)
		})
	}
}

func TestRunPreemptionInfo_InternalQueueOrder(t *testing.T) {
	// Should sort by costToPreempt, scheduledAtPriority, cost, ageMillis and tie-break on jobId
	result1 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 0, preemptedSiblingJobId: "a"}
	result2 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 0, preemptedSiblingJobId: "b"}
	result3 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 1, preemptedSiblingJobId: "c"}
	result4 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 50, cost: 2, ageMillis: 1, preemptedSiblingJobId: "d"}
	result5 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 50, cost: 2, ageMillis: 2, preemptedSiblingJobId: "e"}
	result6 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 100, cost: 1, ageMillis: 0, preemptedSiblingJobId: "f"}
	result7 := &preemptibleGang{costToPreempt: 1, scheduledAtPriority: 100, cost: 2, ageMillis: 0, preemptedSiblingJobId: "g"}
	result8 := &preemptibleGang{costToPreempt: 2, scheduledAtPriority: 10, cost: 0, ageMillis: 0, preemptedSiblingJobId: "h"}
	result9 := &preemptibleGang{costToPreempt: 2, scheduledAtPriority: 10, cost: 0, ageMillis: 10, preemptedSiblingJobId: "i"}
	result10 := &preemptibleGang{costToPreempt: 2, scheduledAtPriority: 10, cost: 5, ageMillis: 0, preemptedSiblingJobId: "j"}

	items := []*preemptibleGang{result2, result1, result4, result5, result3, result9, result8, result10, result7, result6}
	expected := []*preemptibleGang{result1, result2, result3, result4, result5, result6, result7, result8, result9, result10}

	sort.Sort(internalQueueOrder(items))
	assert.Equal(t, expected, items)
}

func TestRunPreemptionInfo_GlobalPreemptionOrder(t *testing.T) {
	// Sort by cross-pool jobs first as they should be preempetd before any job first even if it is unfair
	// Then sort by priorityPreemption, even if it is otherwise unfair, this represents urgency based preemption and is inherently unfair
	// Then sort by largest weightedCostAfterPreemption, cost, ageMillis and tie-break on jobId, unless same queue then sort on queuePreemptedOrdinal4
	result1 := &preemptibleGang{crossPool: true, queue: "e-away", weightedCostAfterPreemption: 0.6, cost: 0.4, ageMillis: 5, preemptedSiblingJobId: "i", queuePreemptedOrdinal: 1}
	result2 := &preemptibleGang{crossPool: true, queue: "a-away", weightedCostAfterPreemption: 0.5, cost: 0.1, ageMillis: 5, preemptedSiblingJobId: "j", queuePreemptedOrdinal: 1}
	result3 := &preemptibleGang{crossPool: true, queue: "e-away", weightedCostAfterPreemption: 0.4, cost: 0.2, ageMillis: 5, preemptedSiblingJobId: "k", queuePreemptedOrdinal: 2}
	result4 := &preemptibleGang{priorityPreemption: true, queue: "e", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, preemptedSiblingJobId: "h", queuePreemptedOrdinal: 1}
	result5 := &preemptibleGang{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 1.1, cost: 0.1, ageMillis: 0, preemptedSiblingJobId: "a", queuePreemptedOrdinal: 1}
	result6 := &preemptibleGang{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 1.0, cost: 0.1, ageMillis: 0, preemptedSiblingJobId: "b", queuePreemptedOrdinal: 2}
	result7 := &preemptibleGang{priorityPreemption: false, queue: "b", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 0, preemptedSiblingJobId: "d", queuePreemptedOrdinal: 1}
	result8 := &preemptibleGang{priorityPreemption: false, queue: "c", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, preemptedSiblingJobId: "f", queuePreemptedOrdinal: 1}
	result9 := &preemptibleGang{priorityPreemption: false, queue: "d", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, preemptedSiblingJobId: "g", queuePreemptedOrdinal: 1}
	result10 := &preemptibleGang{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 0.9, cost: 0.1, ageMillis: 0, preemptedSiblingJobId: "c", queuePreemptedOrdinal: 3}
	result11 := &preemptibleGang{priorityPreemption: false, queue: "b", weightedCostAfterPreemption: 0.89, cost: 0.01, ageMillis: 0, preemptedSiblingJobId: "e", queuePreemptedOrdinal: 2}

	items := []*preemptibleGang{result11, result3, result2, result5, result1, result10, result6, result4, result9, result8, result7}
	expected := []*preemptibleGang{result1, result2, result3, result4, result5, result6, result7, result8, result9, result10, result11}

	sort.Sort(globalPreemptionOrder(items))
	assert.Equal(t, expected, items)
}
