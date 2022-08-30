package store

import (
	"fmt"
	"testing"

	"github.com/G-Research/armada/internal/eventingester/model"
	"github.com/stretchr/testify/assert"
)

type MockEventStore struct {
	Errors       []error
	StoredEvents []*model.Event
}

// If Errors contains errors then MockEventStore will pop the first erorr and return it
// If errors is empty then the vents are recorded
func (es *MockEventStore) ReportEvents(update []*model.Event) error {
	if len(es.Errors) > 0 {
		err := es.Errors[0]
		es.Errors = es.Errors[1:]
		return err
	}
	es.StoredEvents = append(es.StoredEvents, update...)
	return nil
}

func (es *MockEventStore) Reset() {
	es.Errors = nil
	es.StoredEvents = nil
}

func TestHappyPath(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{}
	insert(es, events)
	assert.Equal(t, events, es.StoredEvents)
}

func TestRetryableError(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{Errors: []error{fmt.Errorf("CLUSTERDOWN ")}}
	insert(es, events)
	assert.Equal(t, events, es.StoredEvents)
	assert.Equal(t, 0, len(es.Errors))
}

func TestNonRetryableError(t *testing.T) {
	events := []*model.Event{{Queue: "queue1"}, {Queue: "queue2"}}
	es := &MockEventStore{Errors: []error{fmt.Errorf("some random error")}}
	insert(es, events)
	assert.Equal(t, 0, len(es.StoredEvents))
	assert.Equal(t, 0, len(es.Errors))
}
