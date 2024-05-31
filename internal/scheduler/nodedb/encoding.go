package nodedb

import (
	"encoding/binary"

	"github.com/armadaproject/armada/internal/scheduler/internaltypes"
)

// NodeIndexKey returns a []byte to be used as a key with the NodeIndex memdb index.
// This key should be used for lookup. Use the rounded version below for inserts.
//
// The layout of the key is:
//
// 0            8              16             32    x          x+8
// | nodeTypeId | resources[0] | resources[1] | ... | nodeIndex |
//
// where the numbers indicate byte index.
// NodeIndex ensures each key is unique and so must be unique across all nodes.
//
// The key layout is such that an index ordered first by the nodeTypeId, then resources[0], and so on.
// The byte representation is appended to out, which is returned.
func NodeIndexKey(out []byte, nodeTypeId uint64, resources []int64) []byte {
	out = EncodeUint64(out, nodeTypeId)
	for _, q := range resources {
		out = EncodeInt64(out, q)
	}
	// Because the key returned by this function should be used with a lower-bound operation on allocatable resources
	// we set the nodeIndex to 0.
	out = EncodeUint64(out, 0)
	return out
}

// RoundedNodeIndexKeyFromResourceList works like NodeIndexKey, except that prior to constructing the key
// the i-th resource is rounded down to the closest multiple of resourceResolution[i].
// This rounding makes iterating over nodes with at least some amount of available resources more efficient.
// It also takes as arguments a list of resource names and a resourceList, instead of a list of resources.
func RoundedNodeIndexKeyFromResourceList(
	out []byte,
	nodeTypeId uint64,
	resourceNames []string,
	resourceResolution []int64,
	rl internaltypes.ResourceList,
	nodeIndex uint64,
) []byte {
	out = EncodeUint64(out, nodeTypeId)
	for i, name := range resourceNames {
		resolution := resourceResolution[i]
		q := rl.GetByNameZeroIfMissing(name)
		q = roundQuantityToResolution(q, resolution)
		out = EncodeInt64(out, q)
	}
	out = EncodeUint64(out, nodeIndex)
	return out
}

func roundQuantityToResolution(q int64, resolution int64) int64 {
	return (q / resolution) * resolution
}

// EncodeInt64 returns the canonical byte representation of an int64 used within the nodeDb.
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

	// TODO(albin): It's possible (though unlikely) that this shifting causes nodeType clashes,
	//              since they're computed by hashing labels etc. and so may be big integers.
	//              This would reduce the efficiency of nodeType indexing but shouldn't affect correctness.

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
