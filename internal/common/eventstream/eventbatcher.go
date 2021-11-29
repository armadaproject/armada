package eventstream

import (
	"errors"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

type eventBatchCallback func(events []*Message) error

var defaultCallback = func(events []*Message) error {
	log.Warnf("eventbatcher: default event batch callback used")
	return nil
}

type EventBatcher interface {
	Register(callback eventBatchCallback)
	Report(event *Message) error
	Stop() error
}

type TimedEventBatcher struct {
	batchSize             int
	maxTimeBetweenBatches time.Duration // max time allowed between contiguous callback calls
	timeout               time.Duration // max time to wait for operations that might timeout

	eventCh chan *Message
	stopCh  chan interface{}
	doneCh  chan interface{}

	callback eventBatchCallback
}

func NewTimedEventBatcher(batchSize int, maxTimeBetweenBatches time.Duration, timeout time.Duration) *TimedEventBatcher {
	b := &TimedEventBatcher{
		batchSize:             batchSize,
		maxTimeBetweenBatches: maxTimeBetweenBatches,
		timeout:               timeout,

		callback: defaultCallback,
		eventCh:  make(chan *Message, 10*batchSize),
		stopCh:   make(chan interface{}),
		doneCh:   make(chan interface{}),
	}
	return b
}

// Process is started when the callback is registered
func (b *TimedEventBatcher) Register(callback eventBatchCallback) {
	b.callback = callback
	go b.start()
}

func (b *TimedEventBatcher) Report(event *Message) error {
	reportFn := func() {
		b.eventCh <- event
	}
	err := waitOrTimeout(reportFn, b.timeout)
	if err != nil {
		return fmt.Errorf("error when trying to report event in batcher: %v", err)
	}
	return nil
}

func (b *TimedEventBatcher) Stop() error {
	stopFn := func() {
		b.stopCh <- "stop"
		<-b.doneCh
	}
	err := waitOrTimeout(stopFn, b.timeout)
	if err != nil {
		return fmt.Errorf("error when trying to stop batcher: %v", err)
	}
	return nil
}

func (b *TimedEventBatcher) start() {
	for {
		timer := time.NewTimer(b.maxTimeBetweenBatches)
		var batch []*Message
	batchLoop:
		for i := 0; i < b.batchSize; i++ {
			select {
			case event := <-b.eventCh:
				batch = append(batch, event)
			case <-timer.C:
				break batchLoop
			case <-b.stopCh:
				b.flush(batch)
				b.doneCh <- "done"
				return
			}
		}
		b.flush(batch)
	}
}

func (b *TimedEventBatcher) flush(events []*Message) {
	if len(events) == 0 {
		return
	}

	err := b.callback(events)
	if err != nil {
		log.Errorf("error when flushing events: callback failed: %v", err)
	}
}

func waitOrTimeout(fn func(), timeout time.Duration) error {
	c := make(chan interface{})
	go func() {
		defer close(c)
		fn()
		c <- "done"
	}()
	select {
	case <-c:
		return nil
	case <-time.After(timeout):
		return errors.New("timeout when calling fn")
	}
}
