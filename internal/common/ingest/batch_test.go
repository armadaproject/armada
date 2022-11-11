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
	defaultMaxTimeOut = 1 * time.Second
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
	time.Sleep(1 * time.Second)
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
	inputChan <- 1
	inputChan <- 2
	testClock.Step(2 * time.Second)
	inputChan <- 3
	inputChan <- 4
	testClock.Step(2 * time.Second)
	time.Sleep(1 * time.Second)
	assert.Equal(t, [][]int{{1, 2}, {3, 4}}, output)
	cancel()
}
