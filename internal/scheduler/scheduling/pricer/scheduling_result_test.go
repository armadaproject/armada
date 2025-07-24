package pricer

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
	result1 := &NodeSchedulingResult{price: 0, resultId: "a"}
	result2 := &NodeSchedulingResult{price: 0, resultId: "b"}
	result3 := &NodeSchedulingResult{price: 0, resultId: "c"}
	result4 := &NodeSchedulingResult{price: 100, resultId: "d"}
	result5 := &NodeSchedulingResult{price: 200, resultId: "e"}
	result6 := &NodeSchedulingResult{price: 200, resultId: "f"}

	items := []*NodeSchedulingResult{result2, result3, result1, result6, result4, result5}
	expected := []*NodeSchedulingResult{result1, result2, result3, result4, result5, result6}

	sort.Sort(nodeCostOrder(items))
	assert.Equal(t, expected, items)
}
