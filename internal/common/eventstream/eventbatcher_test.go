package eventstream

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/pkg/api"
)

type callbackWrapper struct {
	invocations [][]*api.EventMessage

	t     *testing.T
	mutex sync.Mutex
}

func NewCallbackWrapper(t *testing.T) *callbackWrapper {
	return &callbackWrapper{
		invocations: make([][]*api.EventMessage, 0),
		t:           t,
	}
}

func (cb *callbackWrapper) callback(events []*api.EventMessage) error {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	cb.invocations = append(cb.invocations, events)
	return nil
}

func (cb *callbackWrapper) firstInvocation() []*api.EventMessage {
	cb.t.Helper()
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	if len(cb.invocations) < 1 {
		assert.FailNow(cb.t, "never called")
		return nil
	}

	return cb.invocations[0]
}

func (cb *callbackWrapper) allInvocations() [][]*api.EventMessage {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	return cb.invocations
}

type mockTimer struct {
	isStopped bool
	c         chan interface{}
}

func newMockTimer() *mockTimer {
	return &mockTimer{
		isStopped: false,
		c:         make(chan interface{}),
	}
}

func (mt *mockTimer) Start() {
	mt.isStopped = false
}

func (mt *mockTimer) Stop() {
	mt.isStopped = true
}

func (mt *mockTimer) Channel() chan interface{} {
	return mt.c
}

func (mt *mockTimer) trySignal() {
	if !mt.isStopped {
		mt.c <- "timer"
	}
	time.Sleep(100*time.Millisecond) // Wait for signal to be processed
}

func TestBatchIsCalledOnce(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t)

	eventBatcher := NewTimeoutEventBatcher(100, newMockTimer())
	eventBatcher.Register(cbWrapper.callback)

	for i := 0; i < 100; i++ {
		err := eventBatcher.Report(makeEvent())
		assert.NoError(t, err)
	}

	assert.Len(t, cbWrapper.allInvocations(), 1)
	assert.Len(t, cbWrapper.firstInvocation(), 100)
}

func TestBatchIsCalledAfterTimeout(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t)
	timer := newMockTimer()

	eventBatcher := NewTimeoutEventBatcher(100, timer)
	eventBatcher.Register(cbWrapper.callback)

	for i := 0; i < 53; i++ {
		err := eventBatcher.Report(makeEvent())
		assert.NoError(t, err)
	}

	assert.Len(t, cbWrapper.allInvocations(), 0)
	timer.trySignal()

	assert.Len(t, cbWrapper.allInvocations(), 1)
	assert.Len(t, cbWrapper.firstInvocation(), 53)
}

func TestFlushBatch(t *testing.T) {
	cbWrapper := NewCallbackWrapper(t)

	eventBatcher := NewTimeoutEventBatcher(100, newMockTimer())
	eventBatcher.Register(cbWrapper.callback)

	for i := 0; i < 53; i++ {
		err := eventBatcher.Report(makeEvent())
		assert.NoError(t, err)
	}

	assert.Len(t, cbWrapper.allInvocations(), 0)
	err := eventBatcher.Flush()
	assert.NoError(t, err)

	assert.Len(t, cbWrapper.allInvocations(), 1)
	assert.Len(t, cbWrapper.firstInvocation(), 53)
}

func makeEvent() *api.EventMessage {
	return &api.EventMessage{Events: nil}
}
