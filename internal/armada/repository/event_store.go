package repository

import "github.com/G-Research/armada/pkg/api"

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}
