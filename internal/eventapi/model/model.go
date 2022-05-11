package model

import (
	"github.com/G-Research/armada/internal/lookoutingester/model"
	"time"
)

// EventRow represents an Event in the Postgres Database
type EventRow struct {
	JobSetId int64
	SeqNo    int64
	Event    []byte
}

// JobsetRow represents a Jobset in the Postgres Database
type JobsetRow struct {
	JobSetId int64
	Queue    string
	Jobset   string
	Created  time.Time
}

// SeqNoRow represents a Sequence Number in the Postgres Database
type SeqNoRow struct {
	JobSetId   int64
	SeqNo      int64
	UpdateTime time.Time
}

// PulsarEventRow represents an Event Row along with information about the originating pulsar message
type PulsarEventRow struct {
	MessageId *model.ConsumerMessageId
	Event     *EventRow
}

// EventSubscription represents a single subscription to a stream of events
type EventSubscription struct {
	SubscriptionId int64
	Channel        chan []*EventRow
}

type EventRequest struct {
	SubscriptionId int64
	Jobset         int64
	Sequence       int64
}

type EventResponse struct {
	SubscriptionId int64
	Events         []*EventRow
}
