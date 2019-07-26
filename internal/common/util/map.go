package util

func GetOrDefault(m map[string]float64, key string, def float64) float64 {
	v, ok := m[key]
	if ok {
		return v
	}
	return def
}
