package maps

import "github.com/armadaproject/armada/internal/common/interfaces"

// FromSlice maps element e of slice into map entry keyFunc(e): valueFunc(e)
func FromSlice[S []E, E any, K comparable, V any](slice S, keyFunc func(E) K, valueFunc func(E) V) map[K]V {
	rv := make(map[K]V, len(slice))
	for _, elem := range slice {
		rv[keyFunc(elem)] = valueFunc(elem)
	}
	return rv
}

// MapValues maps the values of m into valueFunc(v).
func MapValues[M ~map[K]VA, K comparable, VA any, VB any](m M, valueFunc func(VA) VB) map[K]VB {
	rv := make(map[K]VB, len(m))
	for k, v := range m {
		rv[k] = valueFunc(v)
	}
	return rv
}

// MapKeys maps the keys of m into keyFunc(k).
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

// DeepEqual compares two maps for equality using the Equals() method defined on the values.
func DeepEqual[M ~map[K]V, K comparable, V interfaces.Equaler[V]](a, b M) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || !va.Equal(vb) {
			return false
		}
	}
	return true
}

// FilterKeys returns a copy of the provided map with any keys for which predicate returns false removed.
func FilterKeys[M ~map[K]V, K comparable, V any](m M, predicate func(K) bool) M {
	rv := make(M)
	for k, v := range m {
		if predicate(k) {
			rv[k] = v
		}
	}
	return rv
}

// Filter returns a copy of the provided map with any keys for which predicate returns false removed.
func Filter[M ~map[K]V, K comparable, V any](m M, predicate func(K, V) bool) M {
	rv := make(M)
	for k, v := range m {
		if predicate(k, v) {
			rv[k] = v
		}
	}
	return rv
}

// RemoveInPlace removes elements that match keySelector
func RemoveInPlace[K comparable, V any](m map[K]V, keySelector func(K) bool) {
	for k := range m {
		if keySelector(k) {
			delete(m, k)
		}
	}
}

func Keys[K comparable, V any](m map[K]V) []K {
	i := 0
	result := make([]K, len(m))
	for k := range m {
		result[i] = k
		i++
	}
	return result
}
