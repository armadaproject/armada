package optimiser

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSchedulingResult_SchedulingCostOrder(t *testing.T) {
	// Should sort by scheduling cost and tie-break on resultId
	result1 := &schedulingResult{schedulingCost: 0, resultId: "a"}
	result2 := &schedulingResult{schedulingCost: 0, resultId: "b"}
	result3 := &schedulingResult{schedulingCost: 100, resultId: "c"}
	result4 := &schedulingResult{schedulingCost: 200, resultId: "d"}
	result5 := &schedulingResult{schedulingCost: 200, resultId: "e"}

	items := []*schedulingResult{result2, result1, result4, result5, result3}
	expected := []*schedulingResult{result1, result2, result3, result4, result5}

	sort.Sort(schedulingCostOrder(items))
	assert.Equal(t, expected, items)
}

func TestNodeSchedulingResult_NodeCostOrder(t *testing.T) {
	// Should sort by scheduling cost, then maximumQueueImpact and tie-break on resultId
	result1 := &nodeSchedulingResult{schedulingCost: 0, maximumQueueImpact: 2, resultId: "a"}
	result2 := &nodeSchedulingResult{schedulingCost: 0, maximumQueueImpact: 2, resultId: "b"}
	result3 := &nodeSchedulingResult{schedulingCost: 0, maximumQueueImpact: 4, resultId: "c"}
	result4 := &nodeSchedulingResult{schedulingCost: 100, maximumQueueImpact: 0, resultId: "d"}
	result5 := &nodeSchedulingResult{schedulingCost: 200, maximumQueueImpact: 1, resultId: "e"}
	result6 := &nodeSchedulingResult{schedulingCost: 200, maximumQueueImpact: 5, resultId: "f"}

	items := []*nodeSchedulingResult{result2, result3, result1, result6, result4, result5}
	expected := []*nodeSchedulingResult{result1, result2, result3, result4, result5, result6}

	sort.Sort(nodeCostOrder(items))
	assert.Equal(t, expected, items)
}
