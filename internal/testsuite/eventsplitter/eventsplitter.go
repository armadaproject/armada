package eventsplitter

import (
	"context"

	"github.com/armadaproject/armada/pkg/api"
)

// EventSplitter is a service for duplicating incoming events onto several outgoing channels.
type EventSplitter struct {
	in   chan *api.EventMessage
	outs []chan *api.EventMessage
}

func New(in chan *api.EventMessage, outs ...chan *api.EventMessage) *EventSplitter {
	return &EventSplitter{
		in:   in,
		outs: outs,
	}
}

func (srv *EventSplitter) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-srv.in:
			for _, c := range srv.outs {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case c <- e:
				}
			}
		}
	}
}
