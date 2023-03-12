package reporter

import "fmt"

type FakeEventReporter struct {
	ReceivedEvents []EventMessage
	ErrorOnReport  bool
}

func NewFakeEventReporter() *FakeEventReporter {
	return &FakeEventReporter{}
}

func (f *FakeEventReporter) Report(events []EventMessage) error {
	if f.ErrorOnReport {
		return fmt.Errorf("failed to report events")
	}
	f.ReceivedEvents = append(f.ReceivedEvents, events...)
	return nil
}

func (f *FakeEventReporter) QueueEvent(event EventMessage, callback func(error)) {
	e := f.Report([]EventMessage{event})
	callback(e)
}
