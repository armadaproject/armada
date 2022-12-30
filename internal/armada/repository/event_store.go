package repository

import "github.com/G-Research/armada/pkg/api"

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}

type TestEventStore struct {
	ReceivedEvents []*api.EventMessage
}

func (es *TestEventStore) ReportEvents(message []*api.EventMessage) error {
	es.ReceivedEvents = append(es.ReceivedEvents, message...)
	return nil
}
