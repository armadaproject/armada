package math

import "golang.org/x/exp/constraints"

type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr | ~float32 |
		~float64 | ~complex64 | ~complex128
}

// Max returns a if a > b and b otherwise.
func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	} else {
		return b
	}
}
