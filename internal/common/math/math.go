package math

import "golang.org/x/exp/constraints"

// Max returns a if a > b and b otherwise.
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	} else {
		return b
	}
}
