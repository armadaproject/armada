package nodedb

import (
	"encoding/binary"

	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/armadaproject/armada/internal/scheduler/schedulerobjects"
)

// NodeIndexKey returns a []byte to be used as a key with the NodeIndex memdb index with layout
//
// 0            8              16             32
// | nodeTypeId | resources[0] | resources[1] | ... |
//
// where the numbers indicate number of bytes.
//
// The key layout is such that an index ordered first by the nodeTypeId, then resources[0], and so on.
// The byte representation is appended to out, which is returned.
func NodeIndexKey(out []byte, nodeTypeId uint64, resources []resource.Quantity) []byte {
	out = EncodeUint64(out, nodeTypeId)
	for _, q := range resources {
		out = EncodeQuantity(out, q)
	}
	return out
}

// RoundedNodeIndexKeyFromResourceList works like NodeIndexKey, except that prior to constructing a the key
// the i-th resource is rounded down to the closest multiple of resourceResolutionMillis[i].
// It also takes as arguments a list of resource names and a resourceList, instead of a list of resources.
func RoundedNodeIndexKeyFromResourceList(out []byte, nodeTypeId uint64, resourceNames []string, resourceResolutionMillis []int64, rl schedulerobjects.ResourceList) []byte {
	out = EncodeUint64(out, nodeTypeId)
	for i, name := range resourceNames {
		resolution := resourceResolutionMillis[i]
		q := rl.Get(name)
		q = roundQuantityToResolution(q, resolution)
		out = EncodeQuantity(out, q)
	}
	return out
}

func roundQuantityToResolution(q resource.Quantity, resolutionMillis int64) resource.Quantity {
	q.SetMilli((q.MilliValue() / resolutionMillis) * resolutionMillis)
	return q
}

// EncodeQuantity returns the canonical byte representation of a resource.Quantity used within the nodeDb.
// The resulting []byte is such that for two resource.Quantity a and b, a.Cmp(b) = bytes.Compare(enc(a), enc(b)).
// The byte representation is appended to out, which is returned.
func EncodeQuantity(out []byte, val resource.Quantity) []byte {
	// We assume that any quantity we want to compare can be represented as an int64.
	return EncodeInt64(out, val.MilliValue())
}

// EncodeInt64 returns the canonical byte representation of a int64 used within the nodeDb.
// The resulting []byte is such that for two int64 a and b, a.Cmp(b) = bytes.Compare(enc(a), enc(b)).
// The byte representation is appended to out, which is returned.
func EncodeInt64(out []byte, val int64) []byte {
	size := 8
	out = append(out, make([]byte, size)...)

	// This bit flips the usign bit on any sized signed twos-complement integer,
	// which when truncated to a uint of the same size will bias the value such
	// that the maximum negative int becomes 0, and the maximum positive int
	// becomes the maximum positive uint.
	scaled := val ^ int64(-1<<(size*8-1))

	binary.BigEndian.PutUint64(out[len(out)-8:], uint64(scaled))
	return out
}

// EncodeUint64 returns the canonical byte representation of a uint64 used within the nodeDb.
// The resulting []byte is such that for two uint64 a and b, a.Cmp(b) = bytes.Compare(enc(a), enc(b)).
// The byte representation is appended to out, which is returned.
func EncodeUint64(out []byte, val uint64) []byte {
	size := 8
	out = append(out, make([]byte, size)...)
	binary.BigEndian.PutUint64(out[len(out)-size:], val)
	return out
}
