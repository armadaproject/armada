package stringinterner

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
)

// StringInterner deduplicates strings with equal value but different backing arrays,
// thus reducing overall memory usage if many duplicate strings are stored.
//
// StringInterner is backed by an LRU so that only the most recently interned strings are kept.
type StringInterner struct {
	lru *lru.Cache
}

// New return a new *StringInterner backed by a LRU of the given size.
func New(cacheSize uint32) *StringInterner {
	lru, err := lru.New(int(cacheSize))
	if err != nil {
		panic(errors.WithStack(err).Error())
	}
	return &StringInterner{lru: lru}
}

// Intern ensures the string is cached and returns the cached string
func (interner *StringInterner) Intern(s string) string {
	if existing, ok, _ := interner.lru.PeekOrAdd(s, s); ok {
		return existing.(string)
	} else {
		return s
	}
}
