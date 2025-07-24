package schedulerobjects

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/common/pointer"
)

func TestAvailableArmadaResource(t *testing.T) {
	total := *fooBar("10", "8")
	unAllocatable := map[int32]*ResourceList{
		1: fooBar("1", "2"),
		2: fooBar("3", "4"),
	}
	node := &Node{
		TotalResources:         &total,
		UnallocatableResources: unAllocatable,
	}
	result := node.AvailableArmadaResource()
	assert.True(t, fooBar("6", "2").ToComputeResources().Equal(result), result)
}

func TestAvailableArmadaResource_FloorsAtZero(t *testing.T) {
	total := *fooBar("10", "8")
	unAllocatable := map[int32]*ResourceList{
		1: fooBar("1", "10"),
		2: fooBar("3", "20"),
	}
	node := &Node{
		TotalResources:         &total,
		UnallocatableResources: unAllocatable,
	}
	result := node.AvailableArmadaResource()
	assert.True(t, fooBar("6", "0").ToComputeResources().Equal(result), result)
}

func fooBar(foo, bar string) *ResourceList {
	return &ResourceList{
		Resources: map[string]*resource.Quantity{
			"foo": pointer.MustParseResource(foo),
			"bar": pointer.MustParseResource(bar),
		},
	}
}
