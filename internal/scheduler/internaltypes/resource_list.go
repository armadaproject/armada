package internaltypes

import (
	"fmt"

	"golang.org/x/exp/slices"
	k8sResource "k8s.io/apimachinery/pkg/api/resource"
)

type ResourceList struct {
	resources []int64              // immutable, do not change this, return a new struct instead!
	factory   *ResourceListFactory // immutable, do not change this!
}

type Resource struct {
	Name  string
	Value int64
	Scale k8sResource.Scale
}

func (rl ResourceList) Equal(other ResourceList) bool {
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
		}
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
	if rl.IsEmpty() && available.IsEmpty() {
		return "", k8sResource.Quantity{}, k8sResource.Quantity{}, false
	}

	if available.factory != nil && rl.factory != nil && rl.factory != available.factory {
		panic("mismatched ResourceListFactory")
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

func (rl ResourceList) Add(other ResourceList) ResourceList {
	if rl.IsEmpty() {
		return other
	}
	if other.IsEmpty() {
		return rl
	}
	if rl.factory != other.factory {
		panic("mismatched ResourceListFactory")
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = r + other.resources[i]
	}
	return ResourceList{factory: rl.factory, resources: result}
}

func (rl ResourceList) Subtract(other ResourceList) ResourceList {
	if other.IsEmpty() {
		return rl
	}
	if rl.IsEmpty() {
		return other.Negate()
	}
	if rl.factory != other.factory {
		panic("mismatched ResourceListFactory")
	}
	result := make([]int64, len(rl.resources))
	for i, r := range rl.resources {
		result[i] = r - other.resources[i]
	}
	return ResourceList{factory: rl.factory, resources: result}
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
