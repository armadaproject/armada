package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
