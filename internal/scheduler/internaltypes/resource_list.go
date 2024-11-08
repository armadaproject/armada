package internaltypes

import (
	"fmt"
	"math"

	"golang.org/x/exp/slices"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

type ResourceType int

const (
	// A normal k8s resource, such as "memory" or "nvidia.com/gpu"
	Kubernetes ResourceType = iota
	// A floating resource that is not tied to a Kubernetes cluster or node,
	// e.g. "external-storage-connections".
	Floating = iota
)

type ResourceList struct {
	resources []int64              // immutable, do not change this, return a new struct instead!
	factory   *ResourceListFactory // immutable, do not change this!
}

type Resource struct {
	Name  string
	Value int64
	Scale k8sResource.Scale
	Type  ResourceType
}

func (rl ResourceList) Equal(other ResourceList) bool {
	assertSameResourceListFactory(rl.factory, other.factory)
	if rl.IsEmpty() && other.IsEmpty() {
		return true
	}
	if rl.IsEmpty() || other.IsEmpty() {
		return false
	}
	return slices.Equal(rl.resources, other.resources)
}

func (rl ResourceList) String() string {
	if rl.IsEmpty() {
		return "empty"
	}
	result := ""
	for i, name := range rl.factory.indexToName {
		if i > 0 {
			result += " "
		}
		result += fmt.Sprintf("%s=%s", name, rl.asQuantity(i).String())
	}
	return result
}

func (rl ResourceList) GetByName(name string) (int64, error) {
	if rl.IsEmpty() {
		return 0, fmt.Errorf("resource type %s not found as resource list is empty", name)
	}
	index, ok := rl.factory.nameToIndex[name]
	if !ok {
		return 0, fmt.Errorf("resource type %s not found", name)
	}
	return rl.resources[index], nil
}

func (rl ResourceList) GetByNameZeroIfMissing(name string) int64 {
	if rl.IsEmpty() {
		return 0
	}
	index, ok := rl.factory.nameToIndex[name]
	if !ok {
		return 0
	}
	return rl.resources[index]
}

func (rl ResourceList) GetResourceByNameZeroIfMissing(name string) k8sResource.Quantity {
	if rl.IsEmpty() {
		return k8sResource.Quantity{}
	}

	index, ok := rl.factory.nameToIndex[name]
	if !ok {
		return k8sResource.Quantity{}
	}

	return *k8sResource.NewScaledQuantity(rl.resources[index], rl.factory.scales[index])
}

func (rl ResourceList) GetResources() []Resource {
	if rl.IsEmpty() {
		return []Resource{}
	}

	result := make([]Resource, len(rl.resources))
	for i, q := range rl.resources {
		result[i] = Resource{
			Name:  rl.factory.indexToName[i],
			Value: q,
			Scale: rl.factory.scales[i],
			Type:  rl.factory.types[i],
		}
	}
	return result
}

func (rl ResourceList) ToMap() map[string]k8sResource.Quantity {
	if rl.IsEmpty() {
		return map[string]k8sResource.Quantity{}
	}

	result := map[string]k8sResource.Quantity{}
	for i, q := range rl.resources {
		quantity := k8sResource.NewScaledQuantity(q, rl.factory.scales[i])
		result[rl.factory.indexToName[i]] = *quantity
	}
	return result
}

func (rl ResourceList) AllZero() bool {
	if rl.IsEmpty() {
		return true
	}
	for _, r := range rl.resources {
		if r != 0 {
			return false
		}
	}
	return true
}

func (rl ResourceList) FloorAtZero() ResourceList {
	if rl.IsEmpty() {
		return rl
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		if r > 0 {
			result[i] = r
		}
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) HasNegativeValues() bool {
	if rl.IsEmpty() {
		return false
	}
	for _, r := range rl.resources {
		if r < 0 {
			return true
		}
	}
	return false
}

func (rl ResourceList) IsEmpty() bool {
	return rl.factory == nil
}

func (rl ResourceList) Factory() *ResourceListFactory {
	return rl.factory
}

func (rl ResourceList) Exceeds(other ResourceList) bool {
	_, _, _, exceeds := rl.ExceedsAvailable(other)
	return exceeds
}

// ExceedsAvailable
// - if any resource in this ResourceList is greater than the equivalent resource in param available, this function returns
//   - the name of the relevant resource
//   - the amount of the relevant resource in available
//   - the amount of the relevant resource in this ResourceList
//   - true
//
// - if no resources in this ResourceList exceed available, the last return value is false.
// - empty resource lists are considered equivalent to all zero.
func (rl ResourceList) ExceedsAvailable(available ResourceList) (string, k8sResource.Quantity, k8sResource.Quantity, bool) {
	assertSameResourceListFactory(rl.factory, available.factory)

	if rl.IsEmpty() && available.IsEmpty() {
		return "", k8sResource.Quantity{}, k8sResource.Quantity{}, false
	}

	var factory *ResourceListFactory
	if available.IsEmpty() {
		factory = rl.factory
	} else {
		factory = available.factory
	}

	availableResources := resourcesZeroIfEmpty(available.resources, factory)
	requiredResources := resourcesZeroIfEmpty(rl.resources, factory)

	for i, requiredQuantity := range requiredResources {
		availableQuantity := availableResources[i]
		if requiredQuantity > availableQuantity {
			return factory.indexToName[i], *available.asQuantity(i), *rl.asQuantity(i), true
		}
	}
	return "", k8sResource.Quantity{}, k8sResource.Quantity{}, false
}

func (rl ResourceList) OfType(t ResourceType) ResourceList {
	if rl.IsEmpty() {
		return rl
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		if rl.factory.types[i] == t {
			result[i] = r
		}
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) Cap(cap ResourceList) ResourceList {
	assertSameResourceListFactory(rl.factory, cap.factory)
	if rl.IsEmpty() {
		return ResourceList{}
	}
	if cap.IsEmpty() {
		return rl
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = min(r, cap.resources[i])
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) Add(other ResourceList) ResourceList {
	assertSameResourceListFactory(rl.factory, other.factory)
	if rl.IsEmpty() {
		return other
	}
	if other.IsEmpty() {
		return rl
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = r + other.resources[i]
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) Subtract(other ResourceList) ResourceList {
	assertSameResourceListFactory(rl.factory, other.factory)
	if other.IsEmpty() {
		return rl
	}
	if rl.IsEmpty() {
		return other.Negate()
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = r - other.resources[i]
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) Multiply(multipliers ResourceFractionList) ResourceList {
	assertSameResourceListFactory(rl.factory, multipliers.factory)
	if rl.IsEmpty() || multipliers.IsEmpty() {
		return ResourceList{}
	}

	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = multiplyResource(r, multipliers.fractions[i])
	}
	return ResourceList{factory: rl.factory, resources: result}
}

// Divide, return 0 on attempt to divide by 0
func (rl ResourceList) DivideZeroOnError(other ResourceList) ResourceFractionList {
	assertSameResourceListFactory(rl.factory, other.factory)
	if rl.IsEmpty() || other.IsEmpty() {
		return ResourceFractionList{}
	}

	result := make([]float64, len(rl.resources))
	for i, r := range rl.resources {
		denom := other.resources[i]
		if denom != 0 {
			result[i] = float64(r) / float64(denom)
		}
	}
	return ResourceFractionList{factory: rl.factory, fractions: result}
}

func (rl ResourceList) Negate() ResourceList {
	if rl.IsEmpty() {
		return rl
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = -r
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) asQuantity(index int) *k8sResource.Quantity {
	if rl.factory == nil {
		return &k8sResource.Quantity{}
	}
	return k8sResource.NewScaledQuantity(rl.resources[index], rl.factory.scales[index])
}

func resourcesZeroIfEmpty(resources []int64, factory *ResourceListFactory) []int64 {
	if resources == nil {
		return make([]int64, len(factory.indexToName))
	}
	return resources
}

func assertSameResourceListFactory(a, b *ResourceListFactory) {
	if a != nil && b != nil && a != b {
		panic("mismatched ResourceListFactory")
	}
}

func multiplyResource(res int64, multiplier float64) int64 {
	if multiplier == 1.0 {
		// Avoid rounding error in the simple case.
		return res
	}

	// Return max int64 if multiplier is infinity.
	// If res is zero, we assume infinity trumps zero, and return int64 maxValue.
	// This gives the right behavior when the result is used as a cap,
	// as an infinity multiplier means "never apply cap".
	if math.IsInf(multiplier, 0) {
		if (multiplier < 0) == (res < 0) {
			return math.MaxInt64
		} else {
			return math.MinInt64
		}
	}

	return int64(float64(res) * multiplier)
}
