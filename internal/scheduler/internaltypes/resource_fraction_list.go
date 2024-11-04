package internaltypes

import (
	"fmt"
)

type ResourceFractionList struct {
	fractions []float64            // immutable, do not change this, return a new struct instead!
	factory   *ResourceListFactory // immutable, do not change this!
}

func (rfl ResourceFractionList) IsEmpty() bool {
	return rfl.factory == nil
}

func (rfl ResourceFractionList) GetByName(name string) (float64, error) {
	if rfl.IsEmpty() {
		return 0, fmt.Errorf("resource type %s not found as resource fraction list is empty", name)
	}
	index, ok := rfl.factory.nameToIndex[name]
	if !ok {
		return 0, fmt.Errorf("resource type %s not found", name)
	}
	return rfl.fractions[index], nil
}

func (rfl ResourceFractionList) assertSameResourceListFactory(other ResourceList) {
	if rfl.factory != nil && other.factory != nil && rfl.factory != other.factory {
		panic("mismatched ResourceListFactory")
	}
}
