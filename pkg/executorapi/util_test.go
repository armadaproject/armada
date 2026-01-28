package executorapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestComputeResourceFromProtoResources(t *testing.T) {
	oneCpu := resource.MustParse("1")
	oneGi := resource.MustParse("1Gi")

	input := map[string]resource.Quantity{
		"cpu":    resource.MustParse("1"),
		"memory": resource.MustParse("1Gi"),
	}

	expected := &ComputeResource{
		Resources: map[string]*resource.Quantity{
			"cpu":    &oneCpu,
			"memory": &oneGi,
		},
	}
	actual := ComputeResourceFromProtoResources(input)
	assert.Equal(t, actual, expected)
}

func TestResourceListFromProtoResources(t *testing.T) {
	oneCpu := resource.MustParse("1")
	oneGi := resource.MustParse("1Gi")

	input := map[string]*resource.Quantity{
		"cpu":    &oneCpu,
		"memory": &oneGi,
	}
	expected := schedulerobjects.ResourceList{
		Resources: map[string]*resource.Quantity{
			"cpu":    &oneCpu,
			"memory": &oneGi,
		},
	}

	actual := ResourceListFromProtoResources(input)
	assert.Equal(t, actual.Resources, expected.Resources)
}
