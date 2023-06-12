package schedulerobjects

import (
	"encoding/binary"
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

// EncodeQuantity returns the canonical byte representation of a Kubernetes resource.Quantity{}.
func EncodeQuantity(val resource.Quantity) []byte {
	// We assume that any quantity we want to compare can be represented as an int64.
	return EncodeQuantityBuffer(nil, val)
}

// EncodeQuantityBuffer returns the canonical byte representation of a Kubernetes resource.Quantity{}.
// This function appends the byte representation to out.
func EncodeQuantityBuffer(out []byte, val resource.Quantity) []byte {
	// We assume that any quantity we want to compare can be represented as an int64.
	return EncodeInt64Buffer(out, val.MilliValue())
}

func EncodeInt64(val int64) []byte {
	return EncodeInt64Buffer(nil, val)
}

func EncodeInt64Buffer(out []byte, val int64) []byte {
	size := 8
	out = append(out, make([]byte, size)...)

	// This bit flips the sign bit on any sized signed twos-complement integer,
	// which when truncated to a uint of the same size will bias the value such
	// that the maximum negative int becomes 0, and the maximum positive int
	// becomes the maximum positive uint.
	scaled := val ^ int64(-1<<(size*8-1))

	binary.BigEndian.PutUint64(out[len(out)-8:], uint64(scaled))
	return out
}

// ScaleQuantity scales q in-place by a factor f.
// This functions overflows for quantities the milli value of which can't be expressed as an int64.
// E.g., 1Pi is ok, but not 10Pi.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}
