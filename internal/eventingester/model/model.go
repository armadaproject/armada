package model

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

// BatchUpdate represents an Event Row along with information about the originating pulsar message
type BatchUpdate struct {
	MessageIds []pulsar.MessageID
	Events     []*Event
}

func (b *BatchUpdate) GetMessageIDs() []pulsar.MessageID {
	return b.MessageIds
}

type Event struct {
	Queue  string
	Jobset string
	Event  []byte
}
