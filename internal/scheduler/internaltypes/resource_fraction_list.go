package internaltypes

import (
	"fmt"
	"math"
)

type ResourceFractionList struct {
	fractions []float64            // immutable, do not change this, return a new struct instead!
	factory   *ResourceListFactory // immutable, do not change this!
}

func (rfl ResourceFractionList) IsEmpty() bool {
	return rfl.factory == nil
}

func (rfl ResourceFractionList) Multiply(other ResourceFractionList) ResourceFractionList {
	assertSameResourceListFactory(rfl.factory, other.factory)
	if rfl.IsEmpty() || other.IsEmpty() {
		return ResourceFractionList{}
	}

	result := make([]float64, len(rfl.fractions))
	for i, r := range rfl.fractions {
		result[i] = r * other.fractions[i]
	}
	return ResourceFractionList{factory: rfl.factory, fractions: result}
}

func (rfl ResourceFractionList) Max() float64 {
	result := math.Inf(-1)
	for _, val := range rfl.fractions {
		if val > result {
			result = val
		}
	}
	return result
}

func (rfl ResourceFractionList) Min() float64 {
	result := math.Inf(1)
	for _, val := range rfl.fractions {
		if val < result {
			result = val
		}
	}
	return result
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
