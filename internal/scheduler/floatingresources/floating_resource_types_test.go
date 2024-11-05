package floatingresources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

func TestAllPools(t *testing.T) {
	sut := makeSut(t)
	assert.Equal(t, []string{"cpu", "gpu"}, sut.AllPools())
}

func TestGetTotalAvailableForPool(t *testing.T) {
	sut := makeSut(t)
	zero := resource.Quantity{}
	assert.Equal(t, map[string]resource.Quantity{"floating-resource-1": resource.MustParse("200"), "floating-resource-2": resource.MustParse("300")}, sut.GetTotalAvailableForPool("cpu").Resources)
	assert.Equal(t, map[string]resource.Quantity{"floating-resource-1": resource.MustParse("100"), "floating-resource-2": zero}, sut.GetTotalAvailableForPool("gpu").Resources)
	assert.Equal(t, map[string]resource.Quantity{"floating-resource-1": zero, "floating-resource-2": zero}, sut.GetTotalAvailableForPool("some-other-pool").Resources)
}

func TestGetTotalAvailableForPoolInternalTypes(t *testing.T) {
	sut := makeSut(t)

	cpuPool := sut.GetTotalAvailableForPoolInternalTypes("cpu")
	assert.Equal(t, int64(200000), cpuPool.GetByNameZeroIfMissing("floating-resource-1"))
	assert.Equal(t, int64(300000), cpuPool.GetByNameZeroIfMissing("floating-resource-2"))

	gpuPool := sut.GetTotalAvailableForPoolInternalTypes("gpu")
	assert.Equal(t, int64(100000), gpuPool.GetByNameZeroIfMissing("floating-resource-1"))
	assert.Equal(t, int64(0), gpuPool.GetByNameZeroIfMissing("floating-resource-2"))

	notFound := sut.GetTotalAvailableForPoolInternalTypes("some-invalid-value")
	assert.Equal(t, int64(0), notFound.GetByNameZeroIfMissing("floating-resource-1"))
	assert.Equal(t, int64(0), notFound.GetByNameZeroIfMissing("floating-resource-2"))
}

func TestWithinLimits_WhenWithinLimits_ReturnsTrue(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"floating-resource-1": resource.MustParse("199")}},
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenAtLimit_ReturnsTrue(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"floating-resource-1": resource.MustParse("200")}},
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenExceedsLimit_ReturnsFalse(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"floating-resource-1": resource.MustParse("201")}},
	)
	assert.False(t, withinLimits)
	assert.NotEmpty(t, errorMessage)
}

func TestWithinLimits_IgnoresNonFloatingResources(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"some-other-resource": resource.MustParse("1000")}},
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenResourceNotSpecifiedForAPool_ReturnsFalse(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("gpu",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"floating-resource-2": resource.MustParse("1")}},
	)
	assert.False(t, withinLimits)
	assert.NotEmpty(t, errorMessage)
}

func TestWithinLimits_WhenPoolDoesNotExist_ReturnsFalse(t *testing.T) {
	sut := makeSut(t)
	withinLimits, errorMessage := sut.WithinLimits("some-other-pool",
		schedulerobjects.ResourceList{Resources: map[string]resource.Quantity{"floating-resource-1": resource.MustParse("1")}},
	)
	assert.False(t, withinLimits)
	assert.NotEmpty(t, errorMessage)
}

func testConfig() []configuration.FloatingResourceConfig {
	return []configuration.FloatingResourceConfig{
		{
			Name: "floating-resource-1",
			Pools: []configuration.FloatingResourcePoolConfig{
				{
					Name:     "cpu",
					Quantity: resource.MustParse("200"),
				},
				{
					Name:     "gpu",
					Quantity: resource.MustParse("100"),
				},
			},
		},
		{
			Name: "floating-resource-2",
			Pools: []configuration.FloatingResourcePoolConfig{
				{
					Name:     "cpu",
					Quantity: resource.MustParse("300"),
				},
			},
		},
	}
}

func makeSut(t *testing.T) *FloatingResourceTypes {
	rlFactory, err := internaltypes.NewResourceListFactory([]configuration.ResourceType{
		{Name: "cpu"},
	}, testConfig())
	if err != nil {
		panic(err)
	}
	sut, err := NewFloatingResourceTypes(testConfig(), rlFactory)
	assert.Nil(t, err)
	return sut
}
