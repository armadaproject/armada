package eventstream

import (
	log "github.com/sirupsen/logrus"
	"sync"
	"time"

	"github.com/G-Research/armada/pkg/api"
)

type eventBatchCallback func(events []*api.EventMessage) error

type EventBatcher interface {
	Register(callback eventBatchCallback)
	Report(event *api.EventMessage) error
	Flush() error
	Stop()
}

type TimeoutEventBatcher struct {
	batchSize int
	timeout   time.Duration

	batch    []*api.EventMessage
	callback eventBatchCallback

	stopChan  chan interface{}
	flushChan chan interface{}

	mutex sync.Mutex
	timer Timer
}

func NewTimeoutEventBatcher(batchSize int, timer Timer) *TimeoutEventBatcher {
	b := &TimeoutEventBatcher{
		batchSize: batchSize,
		timer:     timer,
		batch:     []*api.EventMessage{},
		stopChan:  make(chan interface{}),
		flushChan: make(chan interface{}),
	}
	go b.start()
	return b
}

func (b *TimeoutEventBatcher) Register(callback eventBatchCallback) {
	b.callback = callback
}

func (b *TimeoutEventBatcher) Report(event *api.EventMessage) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.batch = append(b.batch, event)
	if len(b.batch) >= b.batchSize {
		b.timer.Stop()
		return b.flushUnlocked()
	} else {
		b.startTimer()
	}

	return nil
}

func (b *TimeoutEventBatcher) Flush() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.flushUnlocked()
}

func (b *TimeoutEventBatcher) Stop() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.timer.Stop()
	b.stopChan <- "stop"
}

func (b *TimeoutEventBatcher) flushUnlocked() error {
	err := b.callback(b.batch)
	b.batch = []*api.EventMessage{}

	return err
}

func (b *TimeoutEventBatcher) startTimer() {
	b.timer.Start()

	go func() {
		<-b.timer.Channel()
		b.flushChan <- "flush"
	}()
}

func (b *TimeoutEventBatcher) start() {
	for {
		select {
		case <-b.flushChan:
			err := b.Flush()
			if err != nil {
				log.Errorf("error on flush event buffer after timer: %v", err)
			}
		case <-b.stopChan:
			if b.timer != nil {
				b.timer.Stop()
			}
			return
		}
	}
}

type Timer interface {
	Start()
	Stop()
	Channel() chan interface{}
}

type CustomTimer struct {
	timeout time.Duration

	c chan interface{}
	timer *time.Timer
}

func NewCustomTimer(timeout time.Duration) *CustomTimer {
	return &CustomTimer{
		timeout: timeout,
		c:       make(chan interface{}),
	}
}

func (t *CustomTimer) Start() {
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
	if !t.timer.Stop() {
		<-t.timer.C
	}
}

func (t *CustomTimer) Channel() chan interface{} {
	return t.c
}
