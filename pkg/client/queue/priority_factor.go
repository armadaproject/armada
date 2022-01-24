package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
)

type PriorityFactor float64

// NewPriorityFactor return PriorityFactor using the value of in. If in value
// is lower than 1.0 an error is returned.
func NewPriorityFactor(in float64) (PriorityFactor, error) {
	if in < 1.0 {
		return 0, fmt.Errorf("priority factor cannot be lower than 1.0. Value: %f", in)
	}

	return PriorityFactor(in), nil
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
func (f *PriorityFactor) UnmarshalJSON(data []byte) error {
	var temp float64

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	val, err := NewPriorityFactor(temp)
	if err != nil {
		return err
	}

	*f = val

	return nil
}

// Generate is implementation of https://pkg.go.dev/testing/quick#Generator interface.
// This method is used for writing tests usign https://pkg.go.dev/testing/quick package
func (f PriorityFactor) Generate(rand *rand.Rand, size int) reflect.Value {
	// rand.Float64 generates values in a range [0, 1). Generate will generate
	// Priority factor values in a range [1, 100)
	float := 1.0 + rand.Float64()*99

	return reflect.ValueOf(PriorityFactor(float))
}
