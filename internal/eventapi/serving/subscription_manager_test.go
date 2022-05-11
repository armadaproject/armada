package serving

import (
	"fmt"
	"github.com/G-Research/armada/internal/eventapi/model"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/clock"
	"sync"
	"testing"
	"time"
)

var defaultJobsetId int64 = 10

type DummyDb struct {
	mutex  sync.Mutex
	events []*model.EventRow
}

func (db *DummyDb) GetEvents(requests []*model.EventRequest) ([]*model.EventResponse, error) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	responses := make([]*model.EventResponse, 0)
	for _, request := range requests {
		println(fmt.Sprintf("Getting events from %d", request.Sequence))
		returnedEvents := make([]*model.EventRow, 0)
		for _, event := range db.events {
			if event.JobSetId == request.Jobset && event.SeqNo > request.Sequence {
				returnedEvents = append(returnedEvents, event)
			}
		}
		responses = append(
			responses,
			&model.EventResponse{
				SubscriptionId: request.SubscriptionId,
				Events:         returnedEvents,
			},
		)
	}
	return responses, nil
}

func (db *DummyDb) addEvents(events []*model.EventRow) {
	db.mutex.Lock()
	defer db.mutex.Unlock()
	db.events = append(db.events, events...)
}

// Check that catchup works as expected
// Initial offset is 3
// First fetch returns event 1
// Second fetch returns event 2
func TestCatchup(t *testing.T) {
	testClock := clock.NewFakeClock(time.Now())
	offsetManager := NewSequenceManager(map[int64]int64{defaultJobsetId: 3})

	event1 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 1, Event: nil}
	event2 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 2, Event: nil}
	event3 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 3, Event: nil}

	db := DummyDb{
		mutex:  sync.Mutex{},
		events: []*model.EventRow{event1},
	}
	sm := NewSubscriptionManager(offsetManager, &db, 100, 10*time.Millisecond, 1*time.Second, 2, testClock)
	subscription := sm.Subscribe(defaultJobsetId, 0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	collectedEvents := make([][]*model.EventRow, 0)
	go func() {
		var index = 0
		for event := range subscription.Channel {
			collectedEvents = append(collectedEvents, event)
			if index == 0 {
				db.addEvents([]*model.EventRow{event2, event3})
			}
			if index == 1 {
				wg.Done()
			}
			index++
		}
	}()
	wg.Wait()
	expectedEvents := [][]*model.EventRow{
		{
			event1,
		},
		{
			event2,
			event3,
		},
	}
	assert.Equal(t, expectedEvents, collectedEvents)
}

// Check that polling works as expected
// Initial offset is 0 so the catchup will complete immediately
// After 1 second we update the index to say that data is available
// Events should then be fetched
func TestPoll(t *testing.T) {
	testClock := clock.NewFakeClock(time.Now())

	// Initialise with no events
	sequenceManager := NewSequenceManager(map[int64]int64{defaultJobsetId: 0})

	event1 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 1, Event: nil}
	event2 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 2, Event: nil}
	event3 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 3, Event: nil}

	db := DummyDb{
		mutex:  sync.Mutex{},
		events: []*model.EventRow{event1, event2, event3},
	}
	sm := NewSubscriptionManager(sequenceManager, &db, 100, 10*time.Millisecond, 1*time.Second, 2, testClock)
	subscription := sm.Subscribe(defaultJobsetId, 0)
	wg := sync.WaitGroup{}
	wg.Add(1)
	collectedEvents := make([][]*model.EventRow, 0)
	go func() {
		var index = 0
		for event := range subscription.Channel {
			collectedEvents = append(collectedEvents, event)
			if index == 0 {
				wg.Done()
			}
		}
	}()
	// Wait for a bit- because the index is zero we should collect anything
	time.Sleep(1000)
	assert.Equal(t, 0, len(collectedEvents))

	// now update the index
	sequenceManager.Update(map[int64]int64{defaultJobsetId: 3})

	// Wait for a bit- we haven't moved on the clock so the poll shouldn't happen
	time.Sleep(1000)
	assert.Equal(t, 0, len(collectedEvents))

	// now advance the clock
	testClock.Step(1 * time.Second)
	wg.Wait()
	expectedEvents := [][]*model.EventRow{
		{
			event1, event2, event3,
		},
	}
	assert.Equal(t, expectedEvents, collectedEvents)
}

func TestFromOffset(t *testing.T) {
	testClock := clock.NewFakeClock(time.Now())

	// Initialise with 3 events
	sequenceManager := NewSequenceManager(map[int64]int64{defaultJobsetId: 3})

	event1 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 1, Event: nil}
	event2 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 2, Event: nil}
	event3 := &model.EventRow{JobSetId: defaultJobsetId, SeqNo: 3, Event: nil}

	db := DummyDb{
		mutex:  sync.Mutex{},
		events: []*model.EventRow{event1, event2, event3},
	}
	sm := NewSubscriptionManager(sequenceManager, &db, 100, 10*time.Millisecond, 1*time.Second, 2, testClock)
	subscription := sm.Subscribe(defaultJobsetId, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	collectedEvents := make([][]*model.EventRow, 0)
	go func() {
		var index = 0
		for event := range subscription.Channel {
			collectedEvents = append(collectedEvents, event)
			if index == 0 {
				wg.Done()
			}
		}
	}()
	wg.Wait()
	expectedEvents := [][]*model.EventRow{
		{
			event2, event3, // should only have events 2 and three because subscribed to events after 1
		},
	}
	assert.Equal(t, expectedEvents, collectedEvents)
}
