package maps

// Map maps the values of m into valueFunc(v).
func MapValues[M ~map[K]VA, K comparable, VA any, VB any](m M, valueFunc func(VA) VB) map[K]VB {
	rv := make(map[K]VB, len(m))
	for k, v := range m {
		rv[k] = valueFunc(v)
	}
	return rv
}

// Map maps the keys of m into keyFunc(k).
// Duplicate keys are overwritten.
func MapKeys[M ~map[KA]V, KA comparable, KB comparable, V any](m M, keyFunc func(KA) KB) map[KB]V {
	rv := make(map[KB]V, len(m))
	for k, v := range m {
		rv[keyFunc(k)] = v
	}
	return rv
}

// Map maps the keys and values of m into keyFunc(k) and valueFunc(v), respectively.
// Duplicate keys are overwritten.
func Map[M ~map[KA]VA, KA comparable, VA any, KB comparable, VB any](m M, keyFunc func(KA) KB, valueFunc func(VA) VB) map[KB]VB {
	rv := make(map[KB]VB, len(m))
	for k, v := range m {
		rv[keyFunc(k)] = valueFunc(v)
	}
	return rv
}
