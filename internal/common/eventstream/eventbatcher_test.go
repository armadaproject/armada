package eventstream

import (
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
)

type callbackWrapper struct {
	invocations [][]*Message

	t                *testing.T
	completedChannel chan interface{}
	mutex            sync.Mutex
}

func NewCallbackWrapper(t *testing.T, maxCalls int) *callbackWrapper {
	cbWrapper := &callbackWrapper{
		invocations:      make([][]*Message, 0),
		t:                t,
		completedChannel: make(chan interface{}, maxCalls),
	}
	return cbWrapper
}

func (cb *callbackWrapper) callback(events []*Message) error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.invocations = append(cb.invocations, events)
	select {
	case cb.completedChannel <- "done":
		// OK
	default:
		assert.FailNow(cb.t, "Failed to mark callback invocation as completed")
	}
	return nil
}

func (cb *callbackWrapper) waitForCalls(n int) error {
	for i := 0; i < n; i++ {
		err := waitOrTimeout(func() {
			<-cb.completedChannel
		}, 10*time.Second)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cb *callbackWrapper) firstInvocation() []*Message {
	cb.t.Helper()
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if len(cb.invocations) < 1 {
		assert.FailNow(cb.t, "never called")
		return nil
	}

	return cb.invocations[0]
}

func (cb *callbackWrapper) lastInvocation() []*Message {
	cb.t.Helper()
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if len(cb.invocations) < 1 {
		assert.FailNow(cb.t, "never called")
		return nil
	}

	return cb.invocations[len(cb.invocations)-1]
}

func (cb *callbackWrapper) allInvocations() [][]*Message {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.invocations
}

func TestBatchIsCalledOnce(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t, 1000)

	eventBatcher := NewTimedEventBatcher(100, 1*time.Second, 10*time.Second)
	eventBatcher.Register(cbWrapper.callback)
	defer stop(t, eventBatcher)

	for i := 0; i < 100; i++ {
		err := eventBatcher.Report(makeMessage())
		assert.NoError(t, err)
	}

	err := cbWrapper.waitForCalls(1)
	assert.NoError(t, err)
	assert.Len(t, cbWrapper.allInvocations(), 1)
	assert.Len(t, cbWrapper.firstInvocation(), 100)
}

func TestBatchIsCalledAfterTimeout(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t, 1000)

	eventBatcher := NewTimedEventBatcher(100, 1*time.Second, 10*time.Second)
	eventBatcher.Register(cbWrapper.callback)
	defer stop(t, eventBatcher)

	for i := 0; i < 53; i++ {
		err := eventBatcher.Report(makeMessage())
		assert.NoError(t, err)
	}

	assert.Len(t, cbWrapper.allInvocations(), 0)

	err := cbWrapper.waitForCalls(1)
	assert.NoError(t, err)
	assert.Len(t, cbWrapper.allInvocations(), 1)
	assert.Len(t, cbWrapper.firstInvocation(), 53)
}

func TestMultipleGoroutines(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t, 10000)

	batchSize := 100
	nGoroutines := 4
	nEventsPerGoroutine := 999

	eventBatcher := NewTimedEventBatcher(batchSize, 1*time.Second, 10*time.Second)
	eventBatcher.Register(cbWrapper.callback)
	defer stop(t, eventBatcher)

	var wg sync.WaitGroup
	wg.Add(nGoroutines)

	for i := 0; i < nGoroutines; i++ {
		go func() {
			for j := 0; j < nEventsPerGoroutine; j++ {
				err := eventBatcher.Report(makeMessage())
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	cbCalls := int(math.Ceil(float64(nGoroutines*nEventsPerGoroutine) / float64(batchSize)))
	err := cbWrapper.waitForCalls(cbCalls)
	assert.NoError(t, err)

	err = cbWrapper.waitForCalls(1)
	assert.Error(t, err, "should not be any more calls after last one")

	assert.Len(t, cbWrapper.allInvocations(), cbCalls)
	assert.Len(t, cbWrapper.firstInvocation(), batchSize)
	assert.Len(t, cbWrapper.lastInvocation(), (nEventsPerGoroutine*nGoroutines)%batchSize)
}

func makeMessage() *Message {
	return &Message{
		EventMessage: &api.EventMessage{Events: nil},
		Ack: func() error {
			return nil
		},
	}
}

func stop(t *testing.T, eventBatcher EventBatcher) {
	t.Helper()
	err := eventBatcher.Stop()
	assert.NoError(t, err)
}
