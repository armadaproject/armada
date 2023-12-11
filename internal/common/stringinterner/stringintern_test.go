package stringinterner

import (
	"reflect"
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testStrings     = []string{"foo", "bar", "baz", "alice", "bob"}
	cloned1         = cloneStrings(testStrings)
	cloned2         = cloneStrings(testStrings)
	defaultCapacity = uint32(3)
)

// Test that repeated calls to intern for strings with identical values but different backing arrays result in deduplication
func TestStringInterner_TestIntern(t *testing.T) {
	interner, err := New(defaultCapacity)
	require.NoError(t, err)

	for i := 0; i < len(cloned1); i++ {
		s1 := cloned1[i]
		s2 := cloned2[i]

		// intern the string and confirm that we can retrieve it
		retrieved1 := interner.Intern(s1)
		assert.Equal(t, retrieved1, s1)
		retrieved1Ptr := addr(retrieved1)

		// assert that retrieving it using the same string results in both value and pointer being unchanged
		retrieved2 := interner.Intern(s2)
		retrieved2Ptr := addr(retrieved2)
		assert.Equal(t, s1, retrieved2)
		assert.Equal(t, retrieved1Ptr, retrieved2Ptr)
		assert.NotEqual(t, retrieved2Ptr, addr(s2))
	}
}

// Test that we don't store more strings than capacity
func TestStringInterner_TestCapacity(t *testing.T) {
	interner, err := New(defaultCapacity)
	require.NoError(t, err)

	// intern a string and assert we return the same string
	retrieved1 := interner.Intern(cloned1[0])
	retrievedPtr := addr(retrieved1)
	assert.Equal(t, addr(cloned1[0]), retrievedPtr)

	// intern a some more strings so tha the first interning is evicted
	for i := 0; i < len(cloned1); i++ {
		interner.Intern(cloned1[i])
	}

	// intern the first string again and see we have a reference to the new pointer
	retrieved2 := interner.Intern(cloned2[0])
	retrieved2Ptr := addr(retrieved2)

	assert.Equal(t, addr(cloned2[0]), retrieved2Ptr)
	assert.NotEqual(t, retrievedPtr, retrieved2Ptr)

	assert.Equal(t, 3, interner.lru.Len()) // confirm that our cache hasn't gone over capacity
}

// This horrible thing allows us to check the pointer to the data backing the string
// We need it to determine if two strings have different data even if their values are equal
func addr(s string) uintptr {
	return (*reflect.StringHeader)(unsafe.Pointer(&s)).Data
}

// Clone an array of strings such that the new array has strings with the same value
// but different backing data
func cloneStrings(orig []string) []string {
	cloned := make([]string, len(orig))
	for i, s := range orig {
		cloned[i] = strings.Clone(s)
	}
	return cloned
}
