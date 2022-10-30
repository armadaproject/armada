package ingest

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	defaultMaxItems   = 3
	defaultMaxTimeOut = 1 * time.Second
)

func TestBatch_MaxItems(t *testing.T) {
	inputChan := make(chan int)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch[int](inputChan, defaultMaxItems, defaultMaxTimeOut, testClock)

	// Post 3 items on the input channel without advancing the clock
	// And we should get a single update on the output channel
	inputChan <- 1
	inputChan <- 2
	inputChan <- 3

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received []int = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()

	wg.Wait()
	expected := []int{
		1, 2, 3,
	}
	assert.Equal(t, expected, received)
}

func TestBatch_Time(t *testing.T) {
	inputChan := make(chan int)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch[int](inputChan, defaultMaxItems, defaultMaxTimeOut, testClock)

	// Post two messages on the input channel and advance clock
	// And we should get a single update on the output channel
	inputChan <- 1
	inputChan <- 2

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received []int = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()
	testClock.Step(2 * time.Second)
	wg.Wait()
	expected := []int{
		1, 2,
	}
	assert.Equal(t, expected, received)
}
