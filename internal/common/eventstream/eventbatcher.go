package eventstream

import (
	"errors"
	"fmt"
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
	timeout   time.Duration

	batch    []*api.EventMessage
	callback eventBatchCallback

	mutex sync.Mutex
	t     *time.Timer
}

func NewTimedEventBatcher(batchSize int, timeout time.Duration) *TimedEventBatcher {
	b := &TimedEventBatcher{
		batchSize: batchSize,
		timeout:   timeout,
		batch:     []*api.EventMessage{},
	}
	return b
}

func (b *TimedEventBatcher) Register(callback eventBatchCallback) {
	b.callback = callback
}

func (b *TimedEventBatcher) Report(event *api.EventMessage) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.batch = append(b.batch, event)
	if len(b.batch) >= b.batchSize {
		err := b.flushUnlocked()
		if err != nil {
			return err
		}
	} else {
		if b.t != nil {
			b.t.Stop()
		}
		b.t = time.AfterFunc(1*time.Second, func() {
			err := b.Flush()
			if err != nil {
				log.Errorf("error on flush event buffer after timer: %v", err)
			}
		})
	}
	return nil
}

func (b *TimedEventBatcher) Flush() error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	return b.flushUnlocked()
}

func (b *TimedEventBatcher) Stop() {
	fmt.Println("TRYING TO STOP")
	err := WaitOrTimeout(func() { b.t.Stop() }, 10*time.Second)
	if err != nil {
		log.Errorf("error when trying to stop batcher: %v", err)
	}
	fmt.Println("STOPPED")
}

func WaitOrTimeout(fn func(), timeout time.Duration) error {
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
