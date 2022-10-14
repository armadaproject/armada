package util

import (
	"math/rand"
	"sync"
)

// LockedSource is a random source that is uses a mutex to ensure it is threadsafe
type LockedSource struct {
	lk  sync.Mutex
	src rand.Source
}

func (r *LockedSource) Int63() (n int64) {
	r.lk.Lock()
	n = r.src.Int63()
	r.lk.Unlock()
	return
}

func (r *LockedSource) Seed(seed int64) {
	r.lk.Lock()
	r.src.Seed(seed)
	r.lk.Unlock()
}

// NewThreadsafeRand Returns a *rand.Rand that is safe to share across multiple goroutines
func NewThreadsafeRand(seed int64) *rand.Rand {
	return rand.New(&LockedSource{
		lk:  sync.Mutex{},
		src: rand.NewSource(seed),
	})
}
