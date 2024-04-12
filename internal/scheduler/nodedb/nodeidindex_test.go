package nodedb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestFromObjectValid(t *testing.T) {
	sut := createNodeIdIndex()
	node := makeTestNode("id")
	ok, val, err := sut.FromObject(node)
	assert.True(t, ok)
	assert.Equal(t, []byte{'i', 'd', 0}, val)
	assert.Nil(t, err)
}

func TestFromObjectEmptyId(t *testing.T) {
	sut := createNodeIdIndex()
	node := makeTestNode("")
	ok, val, err := sut.FromObject(node)
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.Nil(t, err)
}

func TestFromObjectWrongType(t *testing.T) {
	sut := createNodeIdIndex()
	ok, val, err := sut.FromObject("this should not be a string")
	assert.False(t, ok)
	assert.Nil(t, val)
	assert.NotNil(t, err)
}

func TestFromArgsValid(t *testing.T) {
	sut := createNodeIdIndex()
	val, err := sut.FromArgs("id")
	assert.Equal(t, []byte{'i', 'd', 0}, val)
	assert.Nil(t, err)
}

func makeTestNode(id string) *internaltypes.Node {
	return internaltypes.CreateNode(id,
		1,
		1,
		"executor",
		"node_name",
		[]v1.Taint{},
		map[string]string{},
		schedulerobjects.ResourceList{},
		schedulerobjects.AllocatableByPriorityAndResourceType{},
		map[string]schedulerobjects.ResourceList{},
		map[string]schedulerobjects.ResourceList{},
		map[string]bool{},
		[][]byte{},
	)
}
