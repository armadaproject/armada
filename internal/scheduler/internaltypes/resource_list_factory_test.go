package internaltypes

import (
	"math"
	"testing"

	v1 "k8s.io/api/core/v1"

	"github.com/stretchr/testify/assert"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/configuration"
)

func TestMakeResourceListFactory(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, []string{"memory", "ephemeral-storage", "cpu", "nvidia.com/gpu"}, factory.indexToName)
	assert.Equal(t, map[string]int{"memory": 0, "ephemeral-storage": 1, "cpu": 2, "nvidia.com/gpu": 3}, factory.nameToIndex)
	assert.Equal(t, []k8sResource.Scale{0, 0, k8sResource.Milli, k8sResource.Milli}, factory.scales)
}

func TestResolutionToScale(t *testing.T) {
	assert.Equal(t, k8sResource.Scale(0), resolutionToScale(k8sResource.MustParse("1")))
	assert.Equal(t, k8sResource.Scale(-3), resolutionToScale(k8sResource.MustParse("0.001")))
	assert.Equal(t, k8sResource.Scale(-3), resolutionToScale(k8sResource.MustParse("0.0011")))
	assert.Equal(t, k8sResource.Scale(-4), resolutionToScale(k8sResource.MustParse("0.00099")))
	assert.Equal(t, k8sResource.Scale(3), resolutionToScale(k8sResource.MustParse("1000")))
}

func TestResolutionToScaleDefaultsCorrectly(t *testing.T) {
	defaultValue := k8sResource.Scale(-3)
	assert.Equal(t, defaultValue, resolutionToScale(k8sResource.MustParse("0")))
	assert.Equal(t, defaultValue, k8sResource.Scale(-3), resolutionToScale(k8sResource.MustParse("-1")))
}

func TestFromNodeProto(t *testing.T) {
	factory := testFactory()
	result := factory.FromNodeProto(map[string]k8sResource.Quantity{
		"memory":  k8sResource.MustParse("100Mi"),
		"cpu":     k8sResource.MustParse("9999999n"),
		"missing": k8sResource.MustParse("200Mi"), // should ignore missing
	})
	assert.Equal(t, int64(100*1024*1024), testGet(&result, "memory"))
	assert.Equal(t, int64(9), testGet(&result, "cpu"))
	assert.Equal(t, int64(0), testGet(&result, "nvidia.com/gpu"))
}

func TestFromJobResourceListFailOnUnknown(t *testing.T) {
	factory := testFactory()
	result, err := factory.FromJobResourceListFailOnUnknown(map[v1.ResourceName]k8sResource.Quantity{
		"memory": k8sResource.MustParse("100Mi"),
		"cpu":    k8sResource.MustParse("9999999n"),
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(100*1024*1024), testGet(&result, "memory"))
	assert.Equal(t, int64(10), testGet(&result, "cpu"))
	assert.Equal(t, int64(0), testGet(&result, "nvidia.com/gpu"))
}

func TestFromJobResourceListFailOnUnknownErrorsIfMissing(t *testing.T) {
	factory := testFactory()
	_, err := factory.FromJobResourceListFailOnUnknown(map[v1.ResourceName]k8sResource.Quantity{
		"memory":  k8sResource.MustParse("100Mi"),
		"missing": k8sResource.MustParse("1"),
	})
	assert.NotNil(t, err)
}

func TestFromJobResourceListIgnoreUnknown(t *testing.T) {
	factory := testFactory()
	result := factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
		"memory": k8sResource.MustParse("100Mi"),
		"cpu":    k8sResource.MustParse("9999999n"),
	})
	assert.Equal(t, int64(100*1024*1024), testGet(&result, "memory"))
	assert.Equal(t, int64(10), testGet(&result, "cpu"))
	assert.Equal(t, int64(0), testGet(&result, "nvidia.com/gpu"))
}

func TestFromJobResourceListIgnoreUnknownDoesNotErrorIfMissing(t *testing.T) {
	factory := testFactory()
	result := factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
		"memory":  k8sResource.MustParse("100Mi"),
		"missing": k8sResource.MustParse("1"),
	})
	assert.Equal(t, int64(100*1024*1024), testGet(&result, "memory"))
}

func TestGetScale(t *testing.T) {
	factory := testFactory()

	scale, err := factory.GetScale("memory")
	assert.Nil(t, err)
	assert.Equal(t, k8sResource.Scale(0), scale)

	scale, err = factory.GetScale("cpu")
	assert.Nil(t, err)
	assert.Equal(t, k8sResource.Milli, scale)
}

func TestGetScaleFailsOnUnknown(t *testing.T) {
	factory := testFactory()

	_, err := factory.GetScale("missing")
	assert.NotNil(t, err)
}

func testFactory() *ResourceListFactory {
	factory, _ := MakeResourceListFactory([]configuration.ResourceType{
		{Name: "memory", Resolution: k8sResource.MustParse("1")},
		{Name: "ephemeral-storage", Resolution: k8sResource.MustParse("1")},
		{Name: "cpu", Resolution: k8sResource.MustParse("1m")},
		{Name: "nvidia.com/gpu", Resolution: k8sResource.MustParse("1m")},
	})
	return factory
}

func testGet(rl *ResourceList, name string) int64 {
	val, err := rl.GetByName(name)
	if err != nil {
		return math.MinInt64
	}
	return val
}
