package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/eventingester/model"
)

type MockEventStore struct {
	Errors       []error
	StoredEvents [][]*model.Event
}

const (
	maxSize   = 1024
	maxEvents = 500
)

// If Errors contains errors then MockEventStore will pop the first error and return it
// If errors is empty then the vents are recorded
func (es *MockEventStore) ReportEvents(update []*model.Event) error {
	if len(es.Errors) > 0 {
		err := es.Errors[0]
		es.Errors = es.Errors[1:]
		return err
	}
	es.StoredEvents = append(es.StoredEvents, update)
	return nil
}

func (es *MockEventStore) Reset() {
	es.Errors = nil
	es.StoredEvents = nil
}

func TestHappyPath(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{}
	insert(es, events, maxSize, maxEvents)
	assert.Equal(t, [][]*model.Event{events}, es.StoredEvents)
}

func TestRetryableError(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{Errors: []error{fmt.Errorf("CLUSTERDOWN ")}}
	insert(es, events, maxSize, maxEvents)
	assert.Equal(t, [][]*model.Event{events}, es.StoredEvents)
	assert.Equal(t, 0, len(es.Errors))
}

func TestNonRetryableError(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{Errors: []error{fmt.Errorf("some random error")}}
	insert(es, events, maxSize, maxEvents)
	assert.Equal(t, 0, len(es.StoredEvents))
	assert.Equal(t, 0, len(es.Errors))
}

func TestSplit(t *testing.T) {
	events := []*model.Event{{Queue: "queue1", Event: []byte{1}}, {Queue: "queue2", Event: []byte{2}}}
	es := &MockEventStore{}

	// No splitting
	insert(es, events, maxSize, maxEvents)
	assert.Equal(t, [][]*model.Event{events}, es.StoredEvents)
	es.Reset()

	// Split by size
	insert(es, events, 1, maxEvents)
	assert.Equal(t, [][]*model.Event{{events[0]}, {events[1]}}, es.StoredEvents)
	es.Reset()

	// split by rows
	insert(es, events, maxSize, 1)
	assert.Equal(t, [][]*model.Event{{events[0]}, {events[1]}}, es.StoredEvents)
}
