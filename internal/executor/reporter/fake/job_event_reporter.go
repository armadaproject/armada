package fake

import "github.com/G-Research/armada/pkg/api"

type FakeEventReporter struct {
	ReceivedEvents []api.Event
}

func (f *FakeEventReporter) Report(event api.Event) error {
	f.ReceivedEvents = append(f.ReceivedEvents, event)
	return nil
}

func (f *FakeEventReporter) QueueEvent(event api.Event, callback func(error)) {
	e := f.Report(event)
	callback(e)
}
