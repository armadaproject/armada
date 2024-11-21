package ingest

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"k8s.io/utils/clock"

	"github.com/armadaproject/armada/internal/common/armadacontext"
)

// Batcher batches up events from a channel.  Batches are created whenever maxItems have been
// received or maxTimeout has elapsed since the last batch was created (whichever occurs first).
type Batcher[T any] struct {
	input      <-chan T
	maxItems   int
	maxTimeout time.Duration
	clock      clock.Clock
	// This function is used to determine how many items are in a given input
	// This allows customising how the batcher batches up your input
	// Such as if you are batching objects A, but want to limit on the number of A.[]B objects seen
	// In which case this function should return len(A.[]B)
	itemCountFunc func(T) int
	publish       chan []T
	buffer        []T
	mutex         sync.Mutex
}

func NewBatcher[T any](input <-chan T, maxItems int, maxTimeout time.Duration, itemCountFunc func(T) int, publish chan []T) *Batcher[T] {
	return &Batcher[T]{
		input:         input,
		maxItems:      maxItems,
		maxTimeout:    maxTimeout,
		itemCountFunc: itemCountFunc,
		publish:       publish,
		clock:         clock.RealClock{},
		mutex:         sync.Mutex{},
	}
}

func (b *Batcher[T]) Run(ctx *armadacontext.Context) {
	for {
		b.buffer = []T{}
		totalNumberOfItems := 0
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
					if totalNumberOfItems > 0 {
						b.publish <- b.buffer
					}
					return
				}
				b.mutex.Lock()
				b.buffer = append(b.buffer, value)
				totalNumberOfItems += b.itemCountFunc(value)
				if totalNumberOfItems >= b.maxItems {
					b.publish <- b.buffer
					appendToBatch = false
				}
				b.mutex.Unlock()
			case <-expire:
				b.mutex.Lock()
				if len(b.buffer) > 0 {
					b.publish <- b.buffer
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
