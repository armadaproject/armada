package mocks

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"

	"github.com/armadaproject/armada/internal/executor/reporter"
)

type FakeEventReporter struct {
	ReceivedEvents []reporter.EventMessage
	ErrorOnReport  bool
	mutex          sync.Mutex
}

func NewFakeEventReporter() *FakeEventReporter {
	return &FakeEventReporter{}
}

func (f *FakeEventReporter) Report(events []reporter.EventMessage) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if f.ErrorOnReport {
		return fmt.Errorf("failed to report events")
	}
	f.ReceivedEvents = append(f.ReceivedEvents, events...)
	return nil
}

// GetReceivedEvents returns a copy of the reported events, safe to read
// while another goroutine is reporting.
func (f *FakeEventReporter) GetReceivedEvents() []reporter.EventMessage {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	events := make([]reporter.EventMessage, len(f.ReceivedEvents))
	copy(events, f.ReceivedEvents)
	return events
}

func (f *FakeEventReporter) QueueEvent(event reporter.EventMessage, callback func(error)) {
	e := f.Report([]reporter.EventMessage{event})
	callback(e)
}

func (f *FakeEventReporter) HasPendingEvents(pod *v1.Pod) bool {
	return false
}
