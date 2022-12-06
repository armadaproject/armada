package util

func GetOrDefault(m map[string]float64, key string, def float64) float64 {
	v, ok := m[key]
	if ok {
		return v
	}
	return def
}

func MergeMaps(a map[string]string, b map[string]string) map[string]string {
	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	for k, v := range b {
		result[k] = v
	}
	return result
}

func DeepCopy(a map[string]string) map[string]string {
	if a == nil {
		return nil
	}

	result := make(map[string]string)
	for k, v := range a {
		result[k] = v
	}
	return result
}

func Equal(a map[string]string, b map[string]string) bool {
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

func FilterKeys(a map[string]string, keys []string) map[string]string {
	if a == nil {
		return nil
	}

	result := make(map[string]string)
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
