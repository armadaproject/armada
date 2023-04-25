package slices

import (
	"fmt"
	"math"
	"math/rand"

	goslices "golang.org/x/exp/slices"
)

// PartitionToLen partitions the elements of s into non-overlapping slices,
// such that each such slice contains at most maxLen elements.
func PartitionToMaxLen[S ~[]E, E any](s S, maxLen int) []S {
	n := int(math.Ceil(float64(len(s)) / float64(maxLen)))
	if n == 0 {
		n = 1
	}
	return Partition(s, n)
}

// Partition partitions the elements of s into n non-overlapping slices,
// such that some slices have len(s)/n+1 items and some len(s)/n items.
// Ordering is preserved, such that Flatten(Partition(s)) is equal to s.
func Partition[S ~[]E, E any](s S, n int) []S {
	if n < 1 {
		panic(fmt.Sprintf("n is %d but must be at least 1", n))
	}
	k := len(s) - (len(s)/n)*n
	rv := make([]S, n)
	i := 0
	for j := 0; j < k; j++ {
		rv[j] = goslices.Clone(s[i : i+len(s)/n+1])
		i += len(s)/n + 1
	}
	for j := k; j < n; j++ {
		rv[j] = goslices.Clone(s[i : i+len(s)/n])
		i += len(s) / n
	}
	return rv
}

// Flatten merges a slice of slices into a single slice.
func Flatten[S ~[]E, E any](s []S) S {
	n := 0
	allNil := true
	for _, si := range s {
		n += len(si)
		allNil = allNil && si == nil
	}
	if allNil {
		return nil
	}
	rv := make(S, n)
	i := 0
	for _, si := range s {
		for _, e := range si {
			rv[i] = e
			i++
		}
	}
	return rv
}

// Concatenate returns a single slice created by concatenating the input slices.
func Concatenate[S ~[]E, E any](s ...S) S {
	return Flatten(s)
}

// Shuffle shuffles s.
func Shuffle[S ~[]E, E any](s ...S) {
	rand.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
}

// Unique returns a copy of s with duplicate elements removed, keeping only the first occurrence.
func Unique[S ~[]E, E comparable](s S) S {
	if s == nil {
		return nil
	}
	rv := make(S, 0)
	seen := make(map[E]bool)
	for _, v := range s {
		if !seen[v] {
			rv = append(rv, v)
			seen[v] = true
		}
	}
	return rv
}

// GroupByFunc groups the elements e_1, ..., e_n of s into separate slices by keyFunc(e).
func GroupByFunc[S ~[]E, E any, K comparable](s S, keyFunc func(E) K) map[K]S {
	rv := make(map[K]S)
	for _, e := range s {
		k := keyFunc(e)
		rv[k] = append(rv[k], e)
	}
	return rv
}

// MapAndGroupByFuncs groups the elements e_1, ..., e_n of s into separate slices by keyFunc(e)
// and then maps those resulting elements by mapFunc(e).
func MapAndGroupByFuncs[S ~[]E, E any, K comparable, V any](s S, keyFunc func(E) K, mapFunc func(E) V) map[K][]V {
	rv := make(map[K][]V)
	for _, e := range s {
		k := keyFunc(e)
		rv[k] = append(rv[k], mapFunc(e))
	}
	return rv
}

func Subtract[T comparable](list []T, toRemove []T) []T {
	if list == nil {
		return nil
	}
	out := make([]T, 0, len(list))

	toRemoveMap := make(map[T]bool, len(toRemove))
	for _, val := range toRemove {
		toRemoveMap[val] = true
	}

	for _, val := range list {
		if !toRemoveMap[val] {
			out = append(out, val)
		}
	}
	return out
}
