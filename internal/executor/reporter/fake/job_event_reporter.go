package fake

import (
	"fmt"

	"github.com/armadaproject/armada/internal/executor/reporter"
)

type FakeEventReporter struct {
	ReceivedEvents []reporter.EventMessage
	ErrorOnReport  bool
}

func NewFakeEventReporter() *FakeEventReporter {
	return &FakeEventReporter{}
}

func (f *FakeEventReporter) Report(events []reporter.EventMessage) error {
	if f.ErrorOnReport {
		return fmt.Errorf("failed to report events")
	}
	f.ReceivedEvents = append(f.ReceivedEvents, events...)
	return nil
}

func (f *FakeEventReporter) QueueEvent(event reporter.EventMessage, callback func(error)) {
	e := f.Report([]reporter.EventMessage{event})
	callback(e)
}
