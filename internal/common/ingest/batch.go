package ingest

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/util/clock"
)

// Batcher batches up events from a channel.  Batches are created whenever maxItems have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
type Batcher[T any] struct {
	input      chan T
	maxItems   int
	maxTimeout time.Duration
	clock      clock.Clock
	callback   func([]T)
}

func NewBatcher[T any](input chan T, maxItems int, maxTimeout time.Duration, callback func([]T)) *Batcher[T] {
	return &Batcher[T]{
		input:      input,
		maxItems:   maxItems,
		maxTimeout: maxTimeout,
		callback:   callback,
		clock:      clock.RealClock{},
	}
}

func (b *Batcher[T]) Run(ctx context.Context) {
	for {
		var batch []T
		expire := b.clock.After(b.maxTimeout)
		for appendToBatch := true; appendToBatch; {
			select {
			case <-ctx.Done():
				println("context is done!")
				// context is finished
				return
			case value, ok := <-b.input:
				if !ok {
					// input channel has closed
					return
				}

				batch = append(batch, value)
				if len(batch) == b.maxItems {
					b.callback(batch)
					appendToBatch = false
				}

			case <-expire:
				if len(batch) > 0 {
					b.callback(batch)
					appendToBatch = false
				}
			}
		}
	}
}
