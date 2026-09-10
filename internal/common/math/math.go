package math

import "cmp"

// Max returns a if a > b and b otherwise.
func Max[T cmp.Ordered](a, b T) T {
	if a > b {
		return a
	} else {
		return b
	}
}
