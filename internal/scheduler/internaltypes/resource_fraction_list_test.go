package internaltypes

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceFractionList_IsEmpty(t *testing.T) {
	factory := testFactory()

	assert.True(t, ResourceFractionList{}.IsEmpty())
	assert.False(t, testResourceFractionList(factory, 0, 0, 0).IsEmpty())
	assert.False(t, testResourceFractionList(factory, 1, 1, 1).IsEmpty())
}

func TestMax(t *testing.T) {
	factory := testFactory()
	assert.Equal(t, 0.0, testResourceFractionList(factory, -0.1, 0.0, 0.0).Max())
	assert.Equal(t, 0.0, testResourceFractionList(factory, 0.0, 0.0, 0.0).Max())
	assert.Equal(t, 0.9, testResourceFractionList(factory, 0.2, 0.9, 0.1).Max())
	assert.Equal(t, 0.9, testResourceFractionList(factory, 0.9, 0.2, 0.1).Max())
}

func TestMax_HandlesEmptyCorrectly(t *testing.T) {
	assert.Equal(t, math.Inf(-1), ResourceFractionList{}.Max())
}

func TestResourceFractionList_Multiply(t *testing.T) {
	factory := testFactory()

	assert.Equal(t,
		testResourceFractionList(factory, 0.125, 0.25, 1),
		testResourceFractionList(factory, 0.25, 0.5, 1).Multiply(
			testResourceFractionList(factory, 0.5, 0.5, 1)))
}

func TestResourceFractionList_Multiply_HandlesEmptyCorrectly(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, ResourceFractionList{}, ResourceFractionList{}.Multiply(ResourceFractionList{}))
	assert.Equal(t, ResourceFractionList{}, ResourceFractionList{}.Multiply(testResourceFractionList(factory, 1, 1, 1)))
	assert.Equal(t, ResourceFractionList{}, testResourceFractionList(factory, 1, 1, 1).Multiply(ResourceFractionList{}))
}

func TestResourceFractionList_GetByName(t *testing.T) {
	factory := testFactory()
	a := testResourceFractionList(factory, 0.1, 0.2, 1)

	cpu, err := a.GetByName("cpu")
	assert.Nil(t, err)
	assert.Equal(t, 0.1, cpu)

	_, err = a.GetByName("missing")
	assert.NotNil(t, err)
}

func TestResourceFractionList_GetByName_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceFractionList{}
	_, err := empty.GetByName("cpu")
	assert.NotNil(t, err)
}
