package ingest

import (
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

func TestBatch_MaxItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	testClock := clock.NewFakeClock(time.Now())
	inputChan := make(chan int)
	output := make([][]int, 0)
	batcher := NewBatcher[int](inputChan, defaultMaxItems, defaultMaxTimeOut, func(a []int) { output = append(output, a) })
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
	waitForExpectedEvents(ctx, &output, 2)
	assert.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}}, output)
	cancel()
}

func TestBatch_Time(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	testClock := clock.NewFakeClock(time.Now())
	inputChan := make(chan int)
	output := make([][]int, 0)
	batcher := NewBatcher[int](inputChan, defaultMaxItems, defaultMaxTimeOut, func(a []int) { output = append(output, a) })
	batcher.clock = testClock

	go func() {
		batcher.Run(ctx)
	}()

	ticker := time.NewTicker(5 * time.Millisecond)

	// start a goroutine that will advance the clock when we have a couple of items waiting
	go func() {
		var done = false
		for !done {
			select {
			case <-ctx.Done():
				done = true
			case <-ticker.C:
				if len(batcher.buffer) == 2 {
					testClock.Step(5 * time.Second)
				}
			}
		}
	}()

	inputChan <- 1
	inputChan <- 2

	waitForExpectedEvents(ctx, &output, 1)
	assert.Equal(t, [][]int{{1, 2}}, output)
	cancel()
}

func waitForExpectedEvents(ctx context.Context, output *[][]int, numEvents int) {
	var done = false
	ticker := time.NewTicker(5 * time.Millisecond)
	for !done {
		select {
		case <-ctx.Done():
			done = true
		case <-ticker.C:
			if len(*output) == numEvents {
				done = true
			}
		}
	}
}
