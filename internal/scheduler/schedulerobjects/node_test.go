package schedulerobjects

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"
)

func TestMarkResourceUnallocatable(t *testing.T) {
	input := Node{
		UnallocatableResources: map[int32]ResourceList{
			10000: makeResourceList("foo", "100"),
		},
		AllocatableByPriorityAndResource: map[int32]ResourceList{
			100:  makeResourceList("foo", "500"),
			1000: makeResourceList("foo", "900"),
		},
	}

	expected := Node{
		UnallocatableResources: map[int32]ResourceList{
			10000:         makeResourceList("foo", "100"),
			math.MaxInt32: makeResourceList("foo", "100"),
		},
		AllocatableByPriorityAndResource: map[int32]ResourceList{
			100:  makeResourceList("foo", "400"),
			1000: makeResourceList("foo", "800"),
		},
	}

	input.MarkResourceUnallocatable(makeResourceList("foo", "100"))
	assertNodeEqual(t, expected, input)
}

func TestMarkResourceUnallocatable_ProtectsFromNegativeValues(t *testing.T) {
	input := Node{
		UnallocatableResources: map[int32]ResourceList{
			10000: makeResourceList("foo", "100"),
		},
		AllocatableByPriorityAndResource: map[int32]ResourceList{
			100:  makeResourceList("foo", "500"),
			1000: makeResourceList("foo", "900"),
		},
	}

	expected := Node{
		UnallocatableResources: map[int32]ResourceList{
			10000:         makeResourceList("foo", "100"),
			math.MaxInt32: makeResourceList("foo", "600"),
		},
		AllocatableByPriorityAndResource: map[int32]ResourceList{
			100:  makeResourceList("foo", "0"),
			1000: makeResourceList("foo", "300"),
		},
	}

	input.MarkResourceUnallocatable(makeResourceList("foo", "600"))
	assertNodeEqual(t, expected, input)
}

func assertNodeEqual(t *testing.T, node1 Node, node2 Node) {
	assert.Equal(t, len(node1.UnallocatableResources), len(node2.UnallocatableResources))
	assert.Equal(t, len(node1.UnallocatableResources), len(node2.UnallocatableResources))
	assert.Equal(t, len(node1.AllocatableByPriorityAndResource), len(node2.AllocatableByPriorityAndResource))
	for priorty, value := range node1.UnallocatableResources {
		assert.Equal(t, len(value.Resources), len(node2.UnallocatableResources[priorty].Resources))
		for resourceName, resourceValue := range value.Resources {
			assert.True(t, resourceValue.Equal(node2.UnallocatableResources[priorty].Resources[resourceName]))
		}

	}
	for priorty, value := range node1.AllocatableByPriorityAndResource {
		assert.Equal(t, len(value.Resources), len(node2.AllocatableByPriorityAndResource[priorty].Resources))
		for resourceName, resourceValue := range value.Resources {
			assert.True(t, resourceValue.Equal(node2.AllocatableByPriorityAndResource[priorty].Resources[resourceName]))
		}
	}
}

func makeResourceList(resourceName string, value string) ResourceList {
	return ResourceList{
		Resources: map[string]resource.Quantity{
			resourceName: resource.MustParse(value),
		},
	}
}
