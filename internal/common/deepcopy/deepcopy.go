package deepcopy

// DeepCopier expresses that the object can be deep-copied.
type DeepCopier[T any] interface {
	// DeepCopy Returns a deep copy of the object.
	DeepCopy() T
}
