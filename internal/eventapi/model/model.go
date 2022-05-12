package model

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/G-Research/armada/internal/pulsarutils"
)

// EventRow represents an Event in the Postgres Database
type EventRow struct {
	JobSetId int64
	SeqNo    int64
	Event    []byte
}

// JobsetRow represents a Jobset in the Postgres Database
// Essentially this is a mapping from (jobsetName, queue) ->  int64 which means
// We can store int64s rather than long strings
type JobsetRow struct {
	JobSetId int64
	Queue    string
	Jobset   string
	Created  time.Time
}

// SeqNoRow represents a Sequence Number in the Postgres Database
// This enables us to keep track of the latest available event for each jobset
type SeqNoRow struct {
	JobSetId   int64
	SeqNo      int64
	UpdateTime time.Time
}

// PulsarEventRow represents an Event Row along with information about the originating pulsar message
type PulsarEventRow struct {
	MessageId *pulsarutils.ConsumerMessageId
	Event     *EventRow
}

// EventSubscription represents a single subscription to a stream of events
type EventSubscription struct {
	SubscriptionId int64
	Channel        chan []*EventRow
}

// EventRequest represents a request for new Event rows form the database
type EventRequest struct {
	SubscriptionId int64 // The subscriber who originated the request
	Jobset         int64 // The id of the jobset they want events for
	Sequence       int64 // Only return  events after this sequence number
}

// EventResponse represents a response to the EventRequest
type EventResponse struct {
	SubscriptionId int64       // The subscriber who originated the request
	Events         []*EventRow // Returned Events
}

// ExternalSeqNo is a sequence number that we pass to end users
// Sequence is the puslar message sequence
// Index is the index of the event inside the armadaevents.EventSequence
type ExternalSeqNo struct {
	Sequence int64
	Index    int
}

// ParseExternalSeqNo Parses an external sequence number which should be of the form "Sequence:Index".
// The empty string will be interpreted as "-1:-1" whoch is the initial sequence numebr
// An error will be returned if the sequence number cannot be parsed
func ParseExternalSeqNo(str string) (*ExternalSeqNo, error) {
	if str == "" {
		return &ExternalSeqNo{-1, -1}, nil
	}
	toks := strings.Split(str, ":")
	if len(toks) != 2 {
		return nil, fmt.Errorf("%s is not a valid sequence number")
	}
	sequence, err := strconv.ParseInt(toks[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number")
	}
	index, err := strconv.Atoi(toks[1])
	if err != nil {
		return nil, fmt.Errorf("%s is not a valid sequence number")
	}
	return &ExternalSeqNo{
		Sequence: sequence,
		Index:    index,
	}, nil
}

// IsValidExternalSeqNo Returns true if the given string is a valid ExternalSeqNo
func IsValidExternalSeqNo(str string) bool {
	_, err := ParseExternalSeqNo(str)
	return err == nil
}

func (e *ExternalSeqNo) ToString() string {
	return fmt.Sprintf("%d:%d", e.Sequence, e.Index)
}

func (e *ExternalSeqNo) IsAfter(other *ExternalSeqNo) bool {
	if other == nil {
		return false
	}
	if e.Sequence > other.Sequence {
		return true
	}
	if e.Sequence == other.Sequence && e.Index > other.Index {
		return true
	}
	return false
}
