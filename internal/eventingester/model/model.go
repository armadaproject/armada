package model

import (
	"github.com/G-Research/armada/internal/pulsarutils"
)

// BatchUpdate represents an Event Row along with information about the originating pulsar message
type BatchUpdate struct {
	MessageIds []*pulsarutils.ConsumerMessageId
	Events     []*Event
}

type Event struct {
	Queue  string
	Jobset string
	Event  []byte
}
