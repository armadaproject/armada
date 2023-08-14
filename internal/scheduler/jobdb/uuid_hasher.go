package jobdb

import (
	"bytes"

	"github.com/google/uuid"
)

// UUIDHasher is an implementation of Hasher for UUID.
type UUIDHasher struct{}

// Hash computes a hash for a UUID.
func (h UUIDHasher) Hash(key uuid.UUID) uint32 {
	var hash uint32
	for _, b := range key {
		hash = hash*31 + uint32(b)
	}
	return hash
}

// Equal checks if two UUIDs are equal.
func (h UUIDHasher) Equal(a, b uuid.UUID) bool {
	return bytes.Equal(a[:], b[:])
}
