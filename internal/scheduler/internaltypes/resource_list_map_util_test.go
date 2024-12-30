package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestRlMapSumValues(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "3", "3Ki"), RlMapSumValues(testMapAllPositive(factory)))
	assert.True(t, RlMapSumValues(testMapEmpty(factory)).IsEmpty())
}

func TestRlMapAllZero(t *testing.T) {
	factory := testFactory()

	assert.False(t, RlMapAllZero(testMapAllPositive(factory)))
	assert.True(t, RlMapAllZero(testMapAllZero(factory)))
	assert.False(t, RlMapAllZero(testMapOneZero(factory)))
	assert.False(t, RlMapAllZero(testMapOneNegative(factory)))
	assert.True(t, RlMapAllZero(testMapEmpty(factory)))
}

func TestRlMapHasNegativeValues(t *testing.T) {
	factory := testFactory()

	assert.False(t, RlMapHasNegativeValues(testMapAllPositive(factory)))
	assert.False(t, RlMapHasNegativeValues(testMapAllZero(factory)))
	assert.False(t, RlMapHasNegativeValues(testMapOneZero(factory)))
	assert.True(t, RlMapHasNegativeValues(testMapOneNegative(factory)))
	assert.False(t, RlMapHasNegativeValues(testMapEmpty(factory)))
}

func TestRlMapFromJobSchedulerObjects(t *testing.T) {
	factory := testFactory()

	input := make(schedulerobjects.QuantityByTAndResourceType[string])
	input.AddResourceList("priorityClass1",
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("1"),
				"memory": resource.MustParse("1Ki"),
			},
		},
	)
	input.AddResourceList("priorityClass2",
		schedulerobjects.ResourceList{
			Resources: map[string]resource.Quantity{
				"cpu":    resource.MustParse("2"),
				"memory": resource.MustParse("2Ki"),
			},
		},
	)

	expected := map[string]ResourceList{
		"priorityClass1": testResourceList(factory, "1", "1Ki"),
		"priorityClass2": testResourceList(factory, "2", "2Ki"),
	}

	assert.Equal(t, expected, RlMapFromJobSchedulerObjects(input, factory))
}

func TestRlMapRemoveZeros(t *testing.T) {
	factory := testFactory()

	input := map[string]ResourceList{
		"priorityClass1": testResourceList(factory, "1", "0Ki"),
		"priorityClass2": testResourceList(factory, "0", "0Ki"),
	}
	expected := map[string]ResourceList{
		"priorityClass1": testResourceList(factory, "1", "0Ki"),
	}
	assert.Equal(t, expected, RlMapRemoveZeros(input))
}

func TestNewAllocatableByPriorityAndResourceType(t *testing.T) {
	factory := testFactory()
	rl := testResourceList(factory, "2", "2Ki")

	result := NewAllocatableByPriorityAndResourceType([]int32{1, 2}, rl)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, int64(2000), result[1].GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(2000), result[2].GetByNameZeroIfMissing("cpu"))
}

func TestMarkAllocated(t *testing.T) {
	factory := testFactory()
	m := map[int32]ResourceList{
		1: testResourceList(factory, "10", "10Gi"),
		2: testResourceList(factory, "20", "20Gi"),
		3: testResourceList(factory, "30", "30Gi"),
		4: testResourceList(factory, "40", "40Gi"),
	}

	expected := map[int32]ResourceList{
		1: testResourceList(factory, "8", "8Gi"),
		2: testResourceList(factory, "18", "18Gi"),
		3: testResourceList(factory, "30", "30Gi"),
		4: testResourceList(factory, "40", "40Gi"),
	}

	MarkAllocated(m, 2, testResourceList(factory, "2", "2Gi"))
	assert.Equal(t, expected, m)
}

func TestMarkAllocatable(t *testing.T) {
	factory := testFactory()
	m := map[int32]ResourceList{
		1: testResourceList(factory, "10", "10Gi"),
		2: testResourceList(factory, "20", "20Gi"),
		3: testResourceList(factory, "30", "30Gi"),
		4: testResourceList(factory, "40", "40Gi"),
	}

	expected := map[int32]ResourceList{
		1: testResourceList(factory, "12", "12Gi"),
		2: testResourceList(factory, "22", "22Gi"),
		3: testResourceList(factory, "30", "30Gi"),
		4: testResourceList(factory, "40", "40Gi"),
	}

	MarkAllocatable(m, 2, testResourceList(factory, "2", "2Gi"))
	assert.Equal(t, expected, m)
}

func testMapAllPositive(factory *ResourceListFactory) map[string]ResourceList {
	return map[string]ResourceList{
		"a": testResourceList(factory, "1", "1Ki"),
		"b": testResourceList(factory, "2", "2Ki"),
	}
}

func testMapAllZero(factory *ResourceListFactory) map[string]ResourceList {
	return map[string]ResourceList{
		"a": testResourceList(factory, "0", "0"),
		"b": testResourceList(factory, "0", "0"),
	}
}

func testMapOneNegative(factory *ResourceListFactory) map[string]ResourceList {
	return map[string]ResourceList{
		"a": testResourceList(factory, "-1", "1Ki"),
		"b": testResourceList(factory, "2", "2Ki"),
	}
}

func testMapOneZero(factory *ResourceListFactory) map[string]ResourceList {
	return map[string]ResourceList{
		"a": testResourceList(factory, "0", "1Ki"),
		"b": testResourceList(factory, "2", "2Ki"),
	}
}

func testMapEmpty(factory *ResourceListFactory) map[string]ResourceList {
	return map[string]ResourceList{}
}
