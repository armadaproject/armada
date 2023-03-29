package interfaces

// DeepCopier expresses that the object can be deep-copied.
type DeepCopier[T any] interface {
	// DeepCopy returns a deep copy of the object.
	DeepCopy() T
}

// Equaler expresses that objects can be compared for equality via the Equals method.
type Equaler[T any] interface {
	// Returns true if both objects are equal.
	Equal(T) bool
}
