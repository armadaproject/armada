package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceFractionList_IsEmpty(t *testing.T) {
	factory := testFactory()

	assert.True(t, ResourceFractionList{}.IsEmpty())
	assert.False(t, testResourceFractionList(factory, 0, 0).IsEmpty())
	assert.False(t, testResourceFractionList(factory, 1, 1).IsEmpty())
}

func TestResourceFractionList_GetByName(t *testing.T) {
	factory := testFactory()
	a := testResourceFractionList(factory, 0.1, 0.2)

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
