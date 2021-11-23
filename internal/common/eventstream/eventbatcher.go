package eventstream

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/pkg/api"
)

type eventBatchCallback func(events []*api.EventMessage) error

type EventBatcher interface {
	Register(callback eventBatchCallback)
	Report(event *api.EventMessage) error
	Flush() error
	Stop()
}

type TimedEventBatcher struct {
	batchSize int

	batch    []*api.EventMessage
	callback eventBatchCallback

	stopChan  chan interface{}
	flushChan chan interface{}
	doneChan  chan interface{}

	mutex sync.Mutex
	timer Timer
}

func NewTimedEventBatcher(batchSize int, timer Timer) *TimedEventBatcher {
	b := &TimedEventBatcher{
		batchSize: batchSize,
		timer:     timer,
		batch:     []*api.EventMessage{},
		stopChan:  make(chan interface{}), // Not buffered, should block if stop signal is sent
		flushChan: make(chan interface{}, 100),
		doneChan:  make(chan interface{}), // Should only signal once
	}
	go b.start()
	return b
}

func (b *TimedEventBatcher) Register(callback eventBatchCallback) {
	b.callback = callback
}

func (b *TimedEventBatcher) Report(event *api.EventMessage) error {
	didFlush, err := b.addAndTryFlush(event)
	if err != nil {
		return err
	}

	if !didFlush {
		b.startTimer()
	}
	return nil
}

func (b *TimedEventBatcher) Flush() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.flushUnlocked()
}

func (b *TimedEventBatcher) Stop() {
	b.stopChan <- "stop"
	<-b.doneChan
}

// Returns whether batch was flushed and any errors
func (b *TimedEventBatcher) addAndTryFlush(event *api.EventMessage) (bool, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.batch = append(b.batch, event)

	shouldFlush := len(b.batch) >= b.batchSize
	var err error
	if shouldFlush {
		err = b.flushUnlocked()
	}
	return shouldFlush, err
}

func (b *TimedEventBatcher) flushUnlocked() error {
	err := b.callback(b.batch)
	b.batch = []*api.EventMessage{}
	return err
}

func (b *TimedEventBatcher) startTimer() {
	b.timer.Start()

	go func() {
		<-b.timer.Channel()
		b.flushChan <- "flush"
	}()
}

func (b *TimedEventBatcher) start() {
	loop:
	for {
		select {
		case <-b.flushChan:
			err := b.Flush()
			if err != nil {
				log.Errorf("error on flush event buffer after timer: %v", err)
			}
		case <-b.stopChan:
			b.timer.Stop()
			break loop
		}
	}
	b.doneChan <- "done"
}

type Timer interface {
	Start()
	Stop()
	Channel() chan interface{}
}

type CustomTimer struct {
	timeout time.Duration

	c     chan interface{}
	timer *time.Timer
	mutex sync.RWMutex
}

func NewCustomTimer(timeout time.Duration) *CustomTimer {
	return &CustomTimer{
		timeout: timeout,
		c:       make(chan interface{}, 100),
	}
}

func (t *CustomTimer) Start() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.timer != nil {
		if !t.timer.Stop() {
			<-t.timer.C
		}
		t.timer.Reset(t.timeout)
	} else {
		t.timer = time.NewTimer(t.timeout)
	}

	go func() {
		<-t.timer.C
		t.c <- "timer"
	}()
}

func (t *CustomTimer) Stop() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if !t.timer.Stop() {
		<-t.timer.C
	}
}

func (t *CustomTimer) Channel() chan interface{} {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	return t.c
}
