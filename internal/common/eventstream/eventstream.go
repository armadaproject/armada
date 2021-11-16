package eventstream

import "github.com/G-Research/armada/pkg/api"

type EventStream interface {
	Publish(events []*api.EventMessage) []error
	Subscribe(callback func(event *api.EventMessage) error) error
	Close() error
}
