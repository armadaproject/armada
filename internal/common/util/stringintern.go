package util

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
)

// StringInterner allows strings with equal values but different backing arrays to be deduplicated
// This is useful in Armada where common strings are often used across jobs (e.g. jobset, queue)
// and so deduplication can help save memory.
// The Interner is backed by an LRU so that only the most recently interned strings are kept.
// Note that this probably isn't the most efficient implementation (eg see https://github.com/josharian/intern
// which abuses sync.pool) but this should be reliable and performance more than good enough for Armada use cases
type StringInterner struct {
	cache *lru.Cache
}

// NewStringInterner will allocate an Interner backed by an LRU limited to the provided size
func NewStringInterner(cacheSize uint32) (*StringInterner, error) {
	cache, err := lru.New(int(cacheSize))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &StringInterner{cache: cache}, nil
}

// Intern ensures the string is cached and returns the cached string
func (i *StringInterner) Intern(s string) string {
	interned, present := i.cache.Get(s)
	if !present {
		interned = s
		i.cache.Add(s, s)

	}
	return interned.(string)
}
