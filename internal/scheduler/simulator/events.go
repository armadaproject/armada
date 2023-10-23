package simulator

import "time"

// Event is a simulator-internal event.
type Event struct {
	// Time at which the event was submitted.
	time time.Time
	// Each event is assigned a sequence number.
	// Events with equal time are ordered by their sequence number.
	sequenceNumber int
	// One of armadaevents.EventSequence or scheduleEvent..
	eventSequenceOrScheduleEvent any
	// Maintained by the heap.Interface methods.
	index int
}

// scheduleEvent is an event indicating the scheduler should be run.
type scheduleEvent struct{}

type EventLog []Event

func (el EventLog) Len() int { return len(el) }

func (el EventLog) Less(i, j int) bool {
	if el[i].time == el[j].time {
		return el[i].sequenceNumber < el[j].sequenceNumber
	}
	return el[j].time.After(el[i].time)
}

func (el EventLog) Swap(i, j int) {
	el[i], el[j] = el[j], el[i]
	el[i].index = i
	el[j].index = j
}

func (el *EventLog) Push(x any) {
	n := len(*el)
	item := x.(Event)
	item.index = n
	*el = append(*el, item)
}

func (el *EventLog) Pop() any {
	old := *el
	n := len(old)
	item := old[n-1]
	old[n-1] = Event{} // avoid memory leak
	item.index = -1    // for safety
	*el = old[0 : n-1]
	return item
}
