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
	assert.Equal(t, "200", qToStr(cpuPool.GetByNameZeroIfMissing("floating-resource-1")))
	assert.Equal(t, "300", qToStr(cpuPool.GetByNameZeroIfMissing("floating-resource-2")))

	gpuPool := sut.GetTotalAvailableForPool("gpu")
	assert.Equal(t, "100", qToStr(gpuPool.GetByNameZeroIfMissing("floating-resource-1")))
	assert.Equal(t, "0", qToStr(gpuPool.GetByNameZeroIfMissing("floating-resource-2")))

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
	allocated := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("199")})
	withinLimits, errorMessage, exceededResource := sut.WithinLimits("cpu", allocated)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
	assert.Empty(t, exceededResource)
}

func TestWithinLimits_WhenAtLimit_ReturnsTrue(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	allocated := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("200")})
	withinLimits, errorMessage, exceededResource := sut.WithinLimits("cpu", allocated)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
	assert.Empty(t, exceededResource)
}

func TestWithinLimits_WhenExceedsLimit_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	allocated := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("201")})
	withinLimits, errorMessage, exceededResource := sut.WithinLimits("cpu", allocated)
	assert.False(t, withinLimits)
	assert.Equal(t, "not enough floating resource floating-resource-1 in pool cpu", errorMessage)
	assert.Equal(t, "floating-resource-1", exceededResource)
}

func TestWithinLimits_IgnoresNonFloatingResources(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	allocated := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"cpu": resource.MustParse("1000")})
	withinLimits, errorMessage, exceededResource := sut.WithinLimits("cpu", allocated)
	assert.True(t, withinLimits)
	assert.Empty(t, errorMessage)
	assert.Empty(t, exceededResource)
}

func TestIsConfiguredForPool_WhenConfigured_ReturnsTrue(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	requested := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("1")})
	configured, reason := sut.IsConfiguredForPool("cpu", requested)
	assert.True(t, configured)
	assert.Empty(t, reason)
}

func TestIsConfiguredForPool_WhenPoolDoesNotExist_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	requested := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{"floating-resource-1": resource.MustParse("1")})
	configured, reason := sut.IsConfiguredForPool("some-other-pool", requested)
	assert.False(t, configured)
	assert.Equal(t, "floating resource floating-resource-1 not configured for pool some-other-pool", reason)
}

func TestIsConfiguredForPool_WhenRequestedResourceNotConfiguredForPool_ReturnsFalse(t *testing.T) {
	rlFactory := makeRlFactory()
	sut := makeSut(t, rlFactory)
	// The gpu pool has floating-resource-1 configured, but not floating-resource-2.
	requested := rlFactory.FromJobResourceListIgnoreUnknown(map[string]resource.Quantity{
		"floating-resource-1": resource.MustParse("1"),
		"floating-resource-2": resource.MustParse("1"),
	})
	configured, reason := sut.IsConfiguredForPool("gpu", requested)
	assert.False(t, configured)
	assert.Equal(t, "floating resource floating-resource-2 not configured for pool gpu", reason)
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

func qToStr(q resource.Quantity) string {
	return q.String()
}
