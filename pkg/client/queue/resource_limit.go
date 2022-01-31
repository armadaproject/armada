package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
)

type ResourceLimit float64

// NewResourceLimit return ResourceLimit using the value of in. If in value
// is not in [0, 1] range an error is returned.
func NewResourceLimit(in float64) (ResourceLimit, error) {
	if in < 0 || in > 1 {
		return 0, fmt.Errorf("resource limit must be in a range [0, 1] (inclusive)")
	}
	return ResourceLimit(in), nil
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
func (rl *ResourceLimit) UnmarshalJSON(data []byte) error {
	var temp float64

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	resourceLimit, err := NewResourceLimit(temp)
	if err != nil {
		return err
	}

	*rl = resourceLimit

	return nil
}

// Generate is implementation of https://pkg.go.dev/testing/quick#Generator interface.
// This method is used for writing tests usign https://pkg.go.dev/testing/quick package
func (ResourceLimit) Generate(rand *rand.Rand, size int) reflect.Value {
	float := rand.Float64()

	return reflect.ValueOf(ResourceLimit(float))
}

type ResourceName string

const (
	ResourceNameCPU    ResourceName = "cpu"
	ResourceNameMemory ResourceName = "memory"
)

// NewResourceName return ResourceName using the value of in. If in value
// is not one of: "cpu", "memory" an error is returned.
func NewResourceName(in string) (ResourceName, error) {
	names := []ResourceName{ResourceNameCPU, ResourceNameMemory}
	for _, name := range names {
		rn := ResourceName(in)
		if rn == name {
			return rn, nil
		}
	}

	return "", fmt.Errorf("resource name string: %s is invalid. Must be one of values: %v", in, names)
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
func (rn *ResourceName) UnmarshalJSON(data []byte) error {
	var temp string

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	resourceName, err := NewResourceName(temp)
	if err != nil {
		return err
	}

	*rn = resourceName

	return nil
}

// Generate is implementation of https://pkg.go.dev/testing/quick#Generator interface.
// This method is used for writing tests usign https://pkg.go.dev/testing/quick package
func (ResourceName) Generate(rand *rand.Rand, size int) reflect.Value {
	resourceNames := []ResourceName{ResourceNameCPU, ResourceNameMemory}
	return reflect.ValueOf(resourceNames[rand.Intn(len(resourceNames))])
}

type ResourceLimits map[ResourceName]ResourceLimit

// NewResourceLimits return ResourceLimits using the value of in. If any of the map values
// is not successfully converted to ResourceLimit an error is returned.
func NewResourceLimits(in map[string]float64) (ResourceLimits, error) {
	out := make(ResourceLimits, len(in))

	for resourceName, resourceLimit := range in {
		name, err := NewResourceName(resourceName)
		if err != nil {
			return nil, err
		}
		limit, err := NewResourceLimit(resourceLimit)
		if err != nil {
			return nil, fmt.Errorf("failed to create limit for resource %s: %s", name, err)
		}
		out[name] = limit
	}

	return out, nil
}
