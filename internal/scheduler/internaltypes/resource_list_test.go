package internaltypes

import (
	"testing"

	"github.com/stretchr/testify/assert"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

func TestEqual(t *testing.T) {
	factory := testFactory()

	a := testResourceList(factory, "1", "1Gi")
	b := testResourceList(factory, "1", "1Gi")
	c := testResourceList(factory, "1", "2Gi")
	assert.True(t, a.Equal(b))
	assert.True(t, a.Equal(a))
	assert.False(t, a.Equal(c))
	assert.False(t, c.Equal(a))
}

func TestEqual_HandlesEmptyCorrectly(t *testing.T) {
	factory := testFactory()

	a := testResourceList(factory, "1", "1Gi")
	e1 := ResourceList{}
	e2 := ResourceList{}

	assert.True(t, e1.Equal(e2))
	assert.True(t, e1.Equal(e1))

	assert.False(t, a.Equal(e1))
	assert.False(t, e1.Equal(a))
}

func TestGetByName(t *testing.T) {
	factory := testFactory()
	a := testResourceList(factory, "1", "1Gi")

	cpu, err := a.GetByName("cpu")
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), cpu)

	_, err = a.GetByName("missing")
	assert.NotNil(t, err)
}

func TestGetByName_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	_, err := empty.GetByName("cpu")
	assert.NotNil(t, err)
}

func TestGetByNameZeroIfMissing(t *testing.T) {
	factory := testFactory()
	a := testResourceList(factory, "1", "1Gi")

	assert.Equal(t, int64(1000), a.GetByNameZeroIfMissing("cpu"))
	assert.Equal(t, int64(0), a.GetByNameZeroIfMissing("missing"))
}

func TestGetByNameZeroIfMissing_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	assert.Equal(t, int64(0), empty.GetByNameZeroIfMissing("missing"))
}

func TestGetResources(t *testing.T) {
	factory := testFactory()
	a := testResourceList(factory, "1", "1Gi")

	expected := []Resource{
		{Name: "memory", Value: 1024 * 1024 * 1024, Scale: k8sResource.Scale(0), Type: Kubernetes},
		{Name: "ephemeral-storage", Value: 0, Scale: k8sResource.Scale(0), Type: Kubernetes},
		{Name: "cpu", Value: 1000, Scale: k8sResource.Milli, Type: Kubernetes},
		{Name: "nvidia.com/gpu", Value: 0, Scale: k8sResource.Milli, Type: Kubernetes},
		{Name: "external-storage-connections", Value: 0, Scale: 0, Type: Floating},
		{Name: "external-storage-bytes", Value: 0, Scale: 0, Type: Floating},
	}
	assert.Equal(t, expected, a.GetResources())
}

func TestGetResources_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	assert.Equal(t, 0, len(empty.GetResources()))
}

func TestToMap(t *testing.T) {
	factory := testFactory()
	a := testResourceList(factory, "1", "1Gi")
	expected := map[string]k8sResource.Quantity{
		"memory":                       *k8sResource.NewScaledQuantity(1024*1024*1024, k8sResource.Scale(0)),
		"ephemeral-storage":            *k8sResource.NewScaledQuantity(0, k8sResource.Scale(0)),
		"cpu":                          *k8sResource.NewScaledQuantity(1000, k8sResource.Milli),
		"nvidia.com/gpu":               *k8sResource.NewScaledQuantity(0, k8sResource.Milli),
		"external-storage-connections": *k8sResource.NewScaledQuantity(0, k8sResource.Scale(0)),
		"external-storage-bytes":       *k8sResource.NewScaledQuantity(0, k8sResource.Scale(0)),
	}
	assert.Equal(t, expected, a.ToMap())
}

func TestToMap_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	assert.Equal(t, map[string]k8sResource.Quantity{}, empty.ToMap())
}

func TestAllZero(t *testing.T) {
	factory := testFactory()
	assert.True(t, testResourceList(factory, "0", "0").AllZero())
	assert.False(t, testResourceList(factory, "1", "0").AllZero())
	assert.False(t, testResourceList(factory, "1", "1").AllZero())
	assert.False(t, testResourceList(factory, "0", "-1").AllZero())
}

func TestAllZero_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	assert.True(t, empty.AllZero())
}

func TestHasNegativeValues(t *testing.T) {
	factory := testFactory()
	assert.False(t, testResourceList(factory, "0", "0").HasNegativeValues())
	assert.False(t, testResourceList(factory, "1", "0").HasNegativeValues())
	assert.True(t, testResourceList(factory, "-1", "1").HasNegativeValues())
	assert.True(t, testResourceList(factory, "-1", "-1").HasNegativeValues())
}

func TestHasNegativeValues_HandlesEmptyCorrectly(t *testing.T) {
	empty := ResourceList{}
	assert.False(t, empty.HasNegativeValues())
}

func TestIsEmpty(t *testing.T) {
	factory := testFactory()

	nonZero := testResourceList(factory, "1", "1")
	zero := testResourceList(factory, "0", "0")
	empty := ResourceList{}

	assert.False(t, nonZero.IsEmpty())
	assert.False(t, zero.IsEmpty())
	assert.True(t, empty.IsEmpty())
}

func TestExceedsAvailable(t *testing.T) {
	factory := testFactory()

	message, _, _, exceeds := testResourceList(factory, "1", "1").ExceedsAvailable(testResourceList(factory, "2", "1"))
	assert.Equal(t, "", message)
	assert.False(t, exceeds)

	message, availableReturned, requiredReturned, exceeds := testResourceList(factory, "10", "1").ExceedsAvailable(testResourceList(factory, "5", "2"))
	assert.Equal(t, "cpu", message)
	assert.True(t, exceeds)
	assert.Equal(t, k8sResource.NewScaledQuantity(10000, k8sResource.Milli), &requiredReturned)
	assert.Equal(t, k8sResource.NewScaledQuantity(5000, k8sResource.Milli), &availableReturned)
}

func TestExceedsAvailable_HandlesEmptyCorrectly(t *testing.T) {
	factory := testFactory()
	empty := ResourceList{}
	notEmpty := testResourceList(factory, "1", "1Ki")
	notEmptyNegative := testResourceList(factory, "-1", "-1Ki")

	message, _, _, exceeds := empty.ExceedsAvailable(empty)
	assert.Equal(t, "", message)
	assert.False(t, exceeds)

	message, _, _, exceeds = empty.ExceedsAvailable(notEmpty)
	assert.Equal(t, "", message)
	assert.False(t, exceeds)

	message, availableReturned, requiredReturned, exceeds := notEmpty.ExceedsAvailable(empty)
	assert.Equal(t, "memory", message)
	assert.True(t, exceeds)
	assert.Equal(t, k8sResource.NewScaledQuantity(1024, 0), &requiredReturned)
	assert.Equal(t, k8sResource.Quantity{}, availableReturned)

	message, _, _, exceeds = notEmptyNegative.ExceedsAvailable(empty)
	assert.Equal(t, "", message)
	assert.False(t, exceeds)

	message, availableReturned, requiredReturned, exceeds = empty.ExceedsAvailable(notEmptyNegative)
	assert.Equal(t, "memory", message)
	assert.True(t, exceeds)
	assert.Equal(t, k8sResource.NewScaledQuantity(-1024, 0), &availableReturned)
	assert.Equal(t, k8sResource.Quantity{}, requiredReturned)
}

func TestOfType(t *testing.T) {
	factory := testFactory()

	rl := factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
		"cpu":                          k8sResource.MustParse("2"),
		"external-storage-connections": k8sResource.MustParse("100"),
	})

	assert.Equal(t,
		factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
			"cpu": k8sResource.MustParse("2"),
		}),
		rl.OfType(Kubernetes))

	assert.Equal(t,
		factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
			"external-storage-connections": k8sResource.MustParse("100"),
		}),
		rl.OfType(Floating))
}

func TestOfType_HandlesEmptyCorrectly(t *testing.T) {
	assert.Equal(t, ResourceList{}, ResourceList{}.OfType(Kubernetes))
	assert.Equal(t, ResourceList{}, ResourceList{}.OfType(Floating))
}

func TestAdd(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "3", "6Ki"), testResourceList(factory, "1", "2Ki").Add(testResourceList(factory, "2", "4Ki")))
	assert.Equal(t, testResourceList(factory, "-1", "-2Ki"), testResourceList(factory, "1", "2Ki").Add(testResourceList(factory, "-2", "-4Ki")))
}

func TestAdd_HandlesEmptyCorrectly(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "1", "1Ki"), testResourceList(factory, "1", "1Ki").Add(ResourceList{}))
	assert.Equal(t, testResourceList(factory, "1", "1Ki"), ResourceList{}.Add(testResourceList(factory, "1", "1Ki")))
	assert.Equal(t, ResourceList{}, ResourceList{}.Add(ResourceList{}))
}

func TestSubtract(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "1", "1Ki"), testResourceList(factory, "2", "2Ki").Subtract(testResourceList(factory, "1", "1Ki")))
	assert.Equal(t, testResourceList(factory, "-1", "-1Ki"), testResourceList(factory, "1", "1Ki").Subtract(testResourceList(factory, "2", "2Ki")))
}

func TestSubtract_HandlesEmptyCorrectly(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "1", "1Ki"), testResourceList(factory, "1", "1Ki").Subtract(ResourceList{}))
	assert.Equal(t, testResourceList(factory, "-1", "-1Ki"), ResourceList{}.Subtract(testResourceList(factory, "1", "1Ki")))
	assert.Equal(t, ResourceList{}, ResourceList{}.Subtract(ResourceList{}))
}

func TestNegate(t *testing.T) {
	factory := testFactory()

	assert.Equal(t, testResourceList(factory, "-1", "-1Ki"), testResourceList(factory, "1", "1Ki").Negate())
	assert.Equal(t, testResourceList(factory, "1", "1Ki"), testResourceList(factory, "-1", "-1Ki").Negate())
}

func TestNegate_HandlesEmptyCorrectly(t *testing.T) {
	assert.Equal(t, ResourceList{}, ResourceList{}.Negate())
}

func testResourceList(factory *ResourceListFactory, cpu string, memory string) ResourceList {
	return factory.FromJobResourceListIgnoreUnknown(map[string]k8sResource.Quantity{
		"cpu":    k8sResource.MustParse(cpu),
		"memory": k8sResource.MustParse(memory),
	})
}
