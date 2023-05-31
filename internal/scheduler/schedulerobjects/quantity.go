package schedulerobjects

import (
	"encoding/binary"
	"math"

	"k8s.io/apimachinery/pkg/api/resource"
)

// EncodeQuantity returns the canonical byte representation of a Kubernetes resource.Quantity{}
// used throughout the scheduler.
func EncodeQuantity(val resource.Quantity) []byte {
	// We assume that any quantity we want to compare can be represented as an int64.
	return encodeInt(val.MilliValue())
}

func encodeInt(val int64) []byte {
	size := 8
	buf := make([]byte, size)

	// This bit flips the sign bit on any sized signed twos-complement integer,
	// which when truncated to a uint of the same size will bias the value such
	// that the maximum negative int becomes 0, and the maximum positive int
	// becomes the maximum positive uint.
	scaled := val ^ int64(-1<<(size*8-1))

	binary.BigEndian.PutUint64(buf, uint64(scaled))
	return buf
}

// scaleQuantity scales q in-place by a factor f.
// This can overflow if the quantity can't be expressed as an int64.
//
// TODO: Test.
func ScaleQuantity(q resource.Quantity, f float64) resource.Quantity {
	q.SetMilli(int64(math.Round(float64(q.MilliValue()) * f)))
	return q
}
