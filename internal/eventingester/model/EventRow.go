package model

import (
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

type EventRow struct {
	JobSetId int64
	Index    int64
	Event    []byte
}

type JobsetRow struct {
	JobSetId int64
	Queue    string
	Jobset   string
}

type Offset struct {
	JobSetId   int64
	Offset     int64
	LastUpdate time.Time
}

type EventMessage struct {
	MessageId pulsar.MessageID
	Event     *armadaevents.EventSequence
}
