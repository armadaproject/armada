package eventsplitter

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/pkg/api"
)

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
	fmt.Println("Splitter started")
	defer fmt.Println("Splitter stopped")
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
