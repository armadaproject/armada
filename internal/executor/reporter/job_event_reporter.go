package reporter

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/clock"

	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/executor/util"
)

type EventReporter interface {
	Report(events []EventMessage) error
	QueueEvent(event EventMessage, callback func(error))
	HasPendingEvents(pod *v1.Pod) bool
}

type queuedEvent struct {
	Event    EventMessage
	Callback func(error)
}

type JobEventReporter struct {
	eventSender      EventSender
	eventBuffer      chan *queuedEvent
	eventQueued      map[string]uint8
	eventQueuedMutex sync.Mutex

	clock        clock.WithTicker
	maxBatchSize int
}

func NewJobEventReporter(
	eventSender EventSender,
	clock clock.WithTicker,
	maxBatchSize int,
) (*JobEventReporter, chan bool) {
	stop := make(chan bool)
	reporter := &JobEventReporter{
		eventSender:      eventSender,
		eventBuffer:      make(chan *queuedEvent, 1000000),
		eventQueued:      map[string]uint8{},
		eventQueuedMutex: sync.Mutex{},
		clock:            clock,
		maxBatchSize:     maxBatchSize,
	}

	go reporter.processEventQueue(stop)

	return reporter, stop
}

func (eventReporter *JobEventReporter) Report(events []EventMessage) error {
	return eventReporter.eventSender.SendEvents(events)
}

func (eventReporter *JobEventReporter) QueueEvent(event EventMessage, callback func(error)) {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	runId := event.JobRunId
	eventReporter.eventQueued[runId] = eventReporter.eventQueued[runId] + 1
	eventReporter.eventBuffer <- &queuedEvent{event, callback}
}

func (eventReporter *JobEventReporter) processEventQueue(stop chan bool) {
	ticker := eventReporter.clock.NewTicker(time.Second * 2)
	toSendBuffer := make([]*queuedEvent, 0, eventReporter.maxBatchSize)
	for {
		select {
		case <-stop:
			for i := len(eventReporter.eventBuffer); i > 0; i -= eventReporter.maxBatchSize {
				batch := eventReporter.fillBatch()
				eventReporter.sendBatch(batch)
			}
			return
		case event := <-eventReporter.eventBuffer:
			toSendBuffer = append(toSendBuffer, event)
			if len(toSendBuffer) >= eventReporter.maxBatchSize {
				eventReporter.sendBatch(toSendBuffer)
				toSendBuffer = nil
			}
		case <-ticker.C():
			if len(toSendBuffer) <= 0 {
				break
			}
			eventReporter.sendBatch(toSendBuffer)
			toSendBuffer = nil
		}
	}
}

func (eventReporter *JobEventReporter) fillBatch(batch ...*queuedEvent) []*queuedEvent {
	for len(batch) < eventReporter.maxBatchSize && len(eventReporter.eventBuffer) > 0 {
		batch = append(batch, <-eventReporter.eventBuffer)
	}
	return batch
}

func (eventReporter *JobEventReporter) sendBatch(batch []*queuedEvent) {
	log.Debugf("Sending batch of %d events", len(batch))
	err := eventReporter.eventSender.SendEvents(queuedEventsToEventMessages(batch))
	go func() {
		for _, e := range batch {
			e.Callback(err)
		}
	}()
	eventReporter.eventQueuedMutex.Lock()
	for _, e := range batch {
		id := e.Event.JobRunId
		count := eventReporter.eventQueued[id]
		if count <= 1 {
			delete(eventReporter.eventQueued, id)
		} else {
			eventReporter.eventQueued[id] = count - 1
		}
	}
	eventReporter.eventQueuedMutex.Unlock()
}

func queuedEventsToEventMessages(queuedEvents []*queuedEvent) []EventMessage {
	result := make([]EventMessage, 0, len(queuedEvents))
	for _, queuedEvent := range queuedEvents {
		result = append(result, queuedEvent.Event)
	}
	return result
}

func (eventReporter *JobEventReporter) HasPendingEvents(pod *v1.Pod) bool {
	eventReporter.eventQueuedMutex.Lock()
	defer eventReporter.eventQueuedMutex.Unlock()
	id := util.ExtractJobRunId(pod)
	return eventReporter.eventQueued[id] > 0
}
