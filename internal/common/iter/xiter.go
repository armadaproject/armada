package iter

import "iter"

// Map returns an iterator over f applied to seq.
func Map[In, Out any](f func(In) Out, seq iter.Seq[In]) iter.Seq[Out] {
	return func(yield func(Out) bool) {
		for in := range seq {
			if !yield(f(in)) {
				return
			}
		}
	}
}

// Concat returns an iterator over the concatenation of the sequences.
func Concat[V any](seqs ...iter.Seq[V]) iter.Seq[V] {
	return func(yield func(V) bool) {
		for _, seq := range seqs {
			for e := range seq {
				if !yield(e) {
					return
				}
			}
		}
	}
}

func Empty[T any]() iter.Seq[T] {
	return func(yield func(T) bool) {}
}
