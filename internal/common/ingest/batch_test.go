package ingest

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	defaultMaxItems   = 3
	defaultMaxTimeOut = 5 * time.Second
)

type resultHolder struct {
	result [][]int
	mutex  sync.Mutex
}

func newResultHolder() *resultHolder {
	return &resultHolder{
		result: make([][]int, 0),
		mutex:  sync.Mutex{},
	}
}

func (r *resultHolder) add(a []int) {
	r.mutex.Lock()
	r.result = append(r.result, a)
	r.mutex.Unlock()
}

func (r *resultHolder) resultLength() int {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return len(r.result)
}

func TestBatch_MaxItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	testClock := clock.NewFakeClock(time.Now())
	inputChan := make(chan int)
	result := newResultHolder()
	batcher := NewBatcher[int](inputChan, defaultMaxItems, defaultMaxTimeOut, result.add)
	batcher.clock = testClock

	go func() {
		batcher.Run(ctx)
	}()

	// Post 3 items on the input channel without advancing the clock
	// And we should get a single update on the output channel
	inputChan <- 1
	inputChan <- 2
	inputChan <- 3
	inputChan <- 4
	inputChan <- 5
	inputChan <- 6
	waitForExpectedEvents(ctx, result, 2)
	assert.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}}, result.result)
	cancel()
}

func TestBatch_Time(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	testClock := clock.NewFakeClock(time.Now())
	inputChan := make(chan int)
	result := newResultHolder()
	batcher := NewBatcher[int](inputChan, defaultMaxItems, defaultMaxTimeOut, result.add)
	batcher.clock = testClock

	go func() {
		batcher.Run(ctx)
	}()

	ticker := time.NewTicker(5 * time.Millisecond)

	// start a goroutine that will advance the clock when we have a couple of items waiting
	go func() {
		done := false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			case <-ticker.C:
				if batcher.BufferLen() == 2 {
					testClock.Step(5 * time.Second)
				}
			}
		}
	}()

	inputChan <- 1
	inputChan <- 2

	waitForExpectedEvents(ctx, result, 1)
	assert.Equal(t, [][]int{{1, 2}}, result.result)
	cancel()
}

func waitForExpectedEvents(ctx context.Context, rh *resultHolder, numEvents int) {
	done := false
	ticker := time.NewTicker(5 * time.Millisecond)
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case <-ticker.C:
			if rh.resultLength() >= numEvents {
				done = true
			}
		}
	}
}
