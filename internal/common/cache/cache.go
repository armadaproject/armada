package cache

import (
	"time"

	"github.com/pkg/errors"
	"go.uber.org/atomic"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// GenericCache holds a value of type T, fetched once at startup and then
// refreshed on a fixed interval.  Any caller to Get will see the most recent
// successful fetch.
type GenericCache[T any] struct {
	updateFrequency time.Duration
	fetchFn         func(ctx *armadacontext.Context) (T, error)

	// we wrap the pointer to T so we can distinguish “never set” from “zero"
	stored *atomic.Pointer[T]
}

// NewGenericCache returns a cache that will call fetchFn initially (via
// Initialise) and then every updateFrequency in its Run loop.
func NewGenericCache[T any](
	fetchFn func(ctx *armadacontext.Context) (T, error),
	updateFrequency time.Duration,
) *GenericCache[T] {
	ptr := atomic.NewPointer[T](nil)
	return &GenericCache[T]{
		updateFrequency: updateFrequency,
		fetchFn:         fetchFn,
		stored:          ptr,
	}
}

// Initialise does one immediate fetch (so Get never errors once Run starts).
func (c *GenericCache[T]) Initialise(ctx *armadacontext.Context) error {
	val, err := c.fetchFn(ctx)
	if err != nil {
		ctx.Errorf("Cache initial fetch failed: %v", err)
		return err
	}
	c.stored.Store(&val)
	return nil
}

// Run will keep refreshing in the background until ctx is Done.
func (c *GenericCache[T]) Run(ctx *armadacontext.Context) error {
	// first fetch if Initialise wasn’t called
	_ = c.Initialise(ctx)

	ticker := time.NewTicker(c.updateFrequency)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			val, err := c.fetchFn(ctx)
			if err != nil {
				ctx.Warnf("Cache refresh failed: %v", err)
			} else {
				c.stored.Store(&val)
				ctx.Infof("Cache refreshed in %s", c.updateFrequency)
			}
		}
	}
}

// Get returns the last‐stored value, or an error if we never successfully
// fetched it yet.
func (c *GenericCache[T]) Get(_ *armadacontext.Context) (T, error) {
	ptr := c.stored.Load()
	if ptr == nil {
		var zero T
		return zero, errors.Errorf("cache not initialized")
	}
	return *ptr, nil
}
