package slices

func Map[T any, U any](list []T, fn func(val T) U) []U {
	out := make([]U, len(list))
	for i, val := range list {
		out[i] = fn(val)
	}
	return out
}

func Filter[T any](list []T, fn func(val T) bool) []T {
	out := make([]T, 0, len(list))
	for _, val := range list {
		if fn(val) {
			out = append(out, val)
		}
	}
	return out
}
