package optimiser

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunPreemptionInfo_InternalQueueOrder(t *testing.T) {
	// Should sort by costToPreempt, scheduledAtPriority, cost, ageMillis and tie-break on jobId
	result1 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 0, jobId: "a"}
	result2 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 0, jobId: "b"}
	result3 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 50, cost: 1, ageMillis: 1, jobId: "c"}
	result4 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 50, cost: 2, ageMillis: 1, jobId: "d"}
	result5 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 50, cost: 2, ageMillis: 2, jobId: "e"}
	result6 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 100, cost: 1, ageMillis: 0, jobId: "f"}
	result7 := &preemptibleJobDetails{costToPreempt: 1, scheduledAtPriority: 100, cost: 2, ageMillis: 0, jobId: "g"}
	result8 := &preemptibleJobDetails{costToPreempt: 2, scheduledAtPriority: 10, cost: 0, ageMillis: 0, jobId: "h"}
	result9 := &preemptibleJobDetails{costToPreempt: 2, scheduledAtPriority: 10, cost: 0, ageMillis: 10, jobId: "i"}
	result10 := &preemptibleJobDetails{costToPreempt: 2, scheduledAtPriority: 10, cost: 5, ageMillis: 0, jobId: "j"}

	items := []*preemptibleJobDetails{result2, result1, result4, result5, result3, result9, result8, result10, result7, result6}
	expected := []*preemptibleJobDetails{result1, result2, result3, result4, result5, result6, result7, result8, result9, result10}

	sort.Sort(internalQueueOrder(items))
	assert.Equal(t, expected, items)
}

func TestRunPreemptionInfo_GlobalPreemptionOrder(t *testing.T) {
	// Sort by cross-pool jobs first as they should be preempetd before any job first even if it is unfair
	// Then sort by priorityPreemption, even if it is otherwise unfair, this represents urgency based preemption and is inherently unfair
	// Then sort by largest weightedCostAfterPreemption, cost, ageMillis and tie-break on jobId, unless same queue then sort on queuePreemptedOrdinal4
	result1 := &preemptibleJobDetails{crossPool: true, queue: "e-away", weightedCostAfterPreemption: 0.6, cost: 0.4, ageMillis: 5, jobId: "i", queuePreemptedOrdinal: 1}
	result2 := &preemptibleJobDetails{crossPool: true, queue: "a-away", weightedCostAfterPreemption: 0.5, cost: 0.1, ageMillis: 5, jobId: "j", queuePreemptedOrdinal: 1}
	result3 := &preemptibleJobDetails{crossPool: true, queue: "e-away", weightedCostAfterPreemption: 0.4, cost: 0.2, ageMillis: 5, jobId: "k", queuePreemptedOrdinal: 2}
	result4 := &preemptibleJobDetails{priorityPreemption: true, queue: "e", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, jobId: "h", queuePreemptedOrdinal: 1}
	result5 := &preemptibleJobDetails{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 1.1, cost: 0.1, ageMillis: 0, jobId: "a", queuePreemptedOrdinal: 1}
	result6 := &preemptibleJobDetails{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 1.0, cost: 0.1, ageMillis: 0, jobId: "b", queuePreemptedOrdinal: 2}
	result7 := &preemptibleJobDetails{priorityPreemption: false, queue: "b", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 0, jobId: "d", queuePreemptedOrdinal: 1}
	result8 := &preemptibleJobDetails{priorityPreemption: false, queue: "c", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, jobId: "f", queuePreemptedOrdinal: 1}
	result9 := &preemptibleJobDetails{priorityPreemption: false, queue: "d", weightedCostAfterPreemption: 0.9, cost: 0.01, ageMillis: 5, jobId: "g", queuePreemptedOrdinal: 1}
	result10 := &preemptibleJobDetails{priorityPreemption: false, queue: "a", weightedCostAfterPreemption: 0.9, cost: 0.1, ageMillis: 0, jobId: "c", queuePreemptedOrdinal: 3}
	result11 := &preemptibleJobDetails{priorityPreemption: false, queue: "b", weightedCostAfterPreemption: 0.89, cost: 0.01, ageMillis: 0, jobId: "e", queuePreemptedOrdinal: 2}

	items := []*preemptibleJobDetails{result11, result3, result2, result5, result1, result10, result6, result4, result9, result8, result7}
	expected := []*preemptibleJobDetails{result1, result2, result3, result4, result5, result6, result7, result8, result9, result10, result11}

	sort.Sort(globalPreemptionOrder(items))
	assert.Equal(t, expected, items)
}
