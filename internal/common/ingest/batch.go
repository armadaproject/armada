package ingest

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/utils/clock"
)

// Batcher batches up events from a channel.  Batches are created whenever maxItems have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
type Batcher[T any] struct {
	input      chan T
	maxItems   int
	maxTimeout time.Duration
	clock      clock.Clock
	callback   func([]T)
	buffer     []T
	mutex      sync.Mutex
}

func NewBatcher[T any](input chan T, maxItems int, maxTimeout time.Duration, callback func([]T)) *Batcher[T] {
	return &Batcher[T]{
		input:      input,
		maxItems:   maxItems,
		maxTimeout: maxTimeout,
		callback:   callback,
		clock:      clock.RealClock{},
		mutex:      sync.Mutex{},
	}
}

func (b *Batcher[T]) Run(ctx context.Context) {
	for {
		b.buffer = []T{}
		expire := b.clock.After(b.maxTimeout)
		for appendToBatch := true; appendToBatch; {
			select {
			case <-ctx.Done():
				log.Info("Batcher: context is done")
				// context is finished
				return
			case value, ok := <-b.input:
				if !ok {
					// input channel has closed
					return
				}
				b.mutex.Lock()
				b.buffer = append(b.buffer, value)
				if len(b.buffer) == b.maxItems {
					b.callback(b.buffer)
					appendToBatch = false
				}
				b.mutex.Unlock()
			case <-expire:
				b.mutex.Lock()
				if len(b.buffer) > 0 {
					b.callback(b.buffer)
				}
				appendToBatch = false
				b.mutex.Unlock()
			}
		}
	}
}

func (b *Batcher[T]) BufferLen() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return len(b.buffer)
}
