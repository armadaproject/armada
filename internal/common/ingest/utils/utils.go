package utils

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/armadaproject/armada/pkg/metricevents"

	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/controlplaneevents"
)

type ArmadaEvent interface {
	*armadaevents.EventSequence | *controlplaneevents.Event | *metricevents.Event
}

// EventsWithIds consists of a batch of type T Events along with the corresponding Pulsar Message Ids
type EventsWithIds[T ArmadaEvent] struct {
	Events     []T
	MessageIds []pulsar.MessageID
}
