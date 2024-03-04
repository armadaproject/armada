package interfaces

import "golang.org/x/exp/constraints"

// DeepCopier represents object that can be deep-copied.
type DeepCopier[T any] interface {
	// DeepCopy returns a deep copy of the object.
	DeepCopy() T
}

// Equaler represents objects can be compared for equality via the Equals method.
type Equaler[T any] interface {
	// Returns true if both objects are equal.
	Equal(T) bool
}

// Number represents any integer or floating-point number.
type Number interface {
	constraints.Integer | constraints.Float
}
