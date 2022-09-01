package batch

import (
	"sync"
	"testing"
	"time"

	"github.com/G-Research/armada/internal/pulsarutils"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"
)

const (
	defaultMaxItems   = 3
	defaultMaxTimeOut = 1 * time.Second
	defaultBufferSize = 3
)

var (
	update1 = &pulsarutils.ConsumerMessage{ConsumerId: 1}
	update2 = &pulsarutils.ConsumerMessage{ConsumerId: 2}
	update3 = &pulsarutils.ConsumerMessage{ConsumerId: 3}
)

func TestBatchByMaxItems(t *testing.T) {
	inputChan := make(chan *pulsarutils.ConsumerMessage)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch(inputChan, defaultMaxItems, defaultMaxTimeOut, defaultBufferSize, testClock)

	// Post 3 instruction sets on the input channel without advancing the clock
	// And we should get a single update on the output channel
	inputChan <- update1
	inputChan <- update2
	inputChan <- update3

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received []*pulsarutils.ConsumerMessage = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()

	wg.Wait()
	expected := []*pulsarutils.ConsumerMessage{
		update1, update2, update3,
	}
	assert.Equal(t, expected, received)
}

func TestBatchByTime(t *testing.T) {
	inputChan := make(chan *pulsarutils.ConsumerMessage)
	testClock := clock.NewFakeClock(time.Now())
	outputChan := Batch(inputChan, defaultMaxItems, defaultMaxTimeOut, defaultBufferSize, testClock)

	// Post two messages on the input channel and advance clock
	// And we should get a single update on the output channel
	inputChan <- update1
	inputChan <- update2

	wg := &sync.WaitGroup{}
	wg.Add(1)

	var received []*pulsarutils.ConsumerMessage = nil

	go func() {
		for e := range outputChan {
			received = e
			close(inputChan)
			wg.Done()
		}
	}()
	testClock.Step(2 * time.Second)
	wg.Wait()
	expected := []*pulsarutils.ConsumerMessage{
		update1, update2,
	}
	assert.Equal(t, expected, received)
}
