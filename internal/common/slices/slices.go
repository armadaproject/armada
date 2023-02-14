package slices

func Map[T any, U any](list []T, fn func(val T) U) []U {
	out := make([]U, len(list))
	for i, val := range list {
		out[i] = fn(val)
	}
	return out
}
