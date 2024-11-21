package floatingresources

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

func TestAllPools(t *testing.T) {
	sut := makeSut(t, makeRlFactory())
	assert.Equal(t, []string{"cpu", "gpu"}, sut.AllPools())
}

func TestNewFloatingResourceTypes_ErrorsOnDuplicateFloatingResource(t *testing.T) {
	cfg := []configuration.FloatingResourceConfig{
		{
			Name: "floating-resource-1",
			Pools: []configuration.FloatingResourcePoolConfig{
				{
					Name:     "cpu",
					Quantity: resource.MustParse("200"),
				},
			},
		},
		{
			Name: "floating-resource-1",
			Pools: []configuration.FloatingResourcePoolConfig{
				{
					Name:     "gpu",
					Quantity: resource.MustParse("300"),
				},
			},
		},
	}

	frt, err := NewFloatingResourceTypes(cfg, makeRlFactory())
	assert.Nil(t, frt)
	assert.NotNil(t, err)
}

func TestNewFloatingResourceTypes_ErrorsOnDuplicatePool(t *testing.T) {
	cfg := []configuration.FloatingResourceConfig{
		{
			Name: "floating-resource-1",
			Pools: []configuration.FloatingResourcePoolConfig{
				{
					Name:     "cpu",
					Quantity: resource.MustParse("200"),
				}, {
					Name:     "cpu",
					Quantity: resource.MustParse("200"),
				},
			},
		},
	}

	frt, err := NewFloatingResourceTypes(cfg, makeRlFactory())
	assert.Nil(t, frt)
	assert.NotNil(t, err)
}

func TestGetTotalAvailableForPool(t *testing.T) {
	sut := makeSut(t, makeRlFactory())

	cpuPool := sut.GetTotalAvailableForPool("cpu")
	assert.Equal(t, int64(200000), cpuPool.GetByNameZeroIfMissing("floating-resource-1"))
	assert.Equal(t, int64(300000), cpuPool.GetByNameZeroIfMissing("floating-resource-2"))

	gpuPool := sut.GetTotalAvailableForPool("gpu")
	assert.Equal(t, int64(100000), gpuPool.GetByNameZeroIfMissing("floating-resource-1"))
	assert.Equal(t, int64(0), gpuPool.GetByNameZeroIfMissing("floating-resource-2"))

	notFound := sut.GetTotalAvailableForPool("some-invalid-value")
	assert.True(t, notFound.IsEmpty())
}

func TestGetTotalAvailableForPoolAsMap(t *testing.T) {
	sut := makeSut(t, makeRlFactory())

	cpuPool := sut.GetTotalAvailableForPoolAsMap("cpu")
	assert.Equal(t, map[string]resource.Quantity{
		"floating-resource-1": *resource.NewMilliQuantity(200000, resource.DecimalSI),
		"floating-resource-2": *resource.NewMilliQuantity(300000, resource.DecimalSI),
	}, cpuPool)

	gpuPool := sut.GetTotalAvailableForPoolAsMap("gpu")
	assert.Equal(t, map[string]resource.Quantity{
		"floating-resource-1": *resource.NewMilliQuantity(100000, resource.DecimalSI),
		"floating-resource-2": *resource.NewMilliQuantity(0, resource.DecimalSI),
	}, gpuPool)

	notFound := sut.GetTotalAvailableForPoolAsMap("some-invalid-value")
	assert.Equal(t, map[string]resource.Quantity{}, notFound)
}

func TestWithinLimits_WhenWithinLimits_ReturnsTrue(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("199")}),
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenAtLimit_ReturnsTrue(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("200")}),
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenExceedsLimit_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("201")}),
	)
	assert.False(t, withinLimits)
	assert.NotEmpty(t, errorMessage)
}

func TestWithinLimits_IgnoresNonFloatingResources(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("cpu",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"cpu": resource.MustParse("1000")}),
	)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
}

func TestWithinLimits_WhenResourceNotSpecifiedForAPool_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("gpu",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-2": resource.MustParse("1")}),
	)
	assert.False(t, withinLimits)
	assert.NotEmpty(t, errorMessage)
}

func TestWithinLimits_WhenPoolDoesNotExist_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	withinLimits, errorMessage := sut.WithinLimits("some-other-pool",
		rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("1")}),
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

func makeRlFactory() *internaltypes.ResourceListFactory {
	rlFactory, err := internaltypes.NewResourceListFactory([]configuration.ResourceType{
		{Name: "cpu"},
	}, testConfig())
	if err != nil {
		panic(err)
	}
	return rlFactory
}

func makeSut(t *testing.T, rlFactory *internaltypes.ResourceListFactory) *FloatingResourceTypes {
	sut, err := NewFloatingResourceTypes(testConfig(), rlFactory)
	assert.Nil(t, err)
	return sut
}
