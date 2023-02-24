package pulsarutils

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
)

// PulsarToChannel is a service for receiving messages from Pulsar and forwarding those on C.
type PulsarToChannel struct {
	Consumer pulsar.Consumer
	C        chan pulsar.Message
}

func NewPulsarToChannel(consumer pulsar.Consumer) *PulsarToChannel {
	return &PulsarToChannel{
		Consumer: consumer,
		C:        make(chan pulsar.Message),
	}
}

// Run starts the service.
func (srv *PulsarToChannel) Run(ctx context.Context) error {
	for {
		msg, err := srv.Consumer.Receive(ctx)
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case srv.C <- msg:
		}
	}
}
