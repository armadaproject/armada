package util

func MergeMaps[K comparable, V any](a map[K]V, b map[K]V) map[K]V {
	result := make(map[K]V)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func Equal[K comparable, V comparable](a map[K]V, b map[K]V) bool {
	if len(a) != len(b) {
		return false
	}

	for key, value := range a {
		if comparativeValue, present := b[key]; !present || value != comparativeValue {
			return false
		}
	}
	return true
}

func FilterKeys[K comparable, V any](a map[K]V, keys []K) map[K]V {
	if a == nil {
		return nil
	}

	result := make(map[K]V)
	for _, key := range keys {
		if val, exists := a[key]; exists {
			result[key] = val
		}
	}

	return result
}

// InverseMap creates a new map where each key: value pair is swapped
// If the same value is present multiple times, a random one will be selected (depending on map key-value iteration)
func InverseMap[K comparable, V comparable](a map[K]V) map[V]K {
	result := make(map[V]K)
	for key, value := range a {
		result[value] = key
	}
	return result
}
