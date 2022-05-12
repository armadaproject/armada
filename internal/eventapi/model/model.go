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
	MessageId *pulsarutils.ConsumerMessageId
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

type ExternalSeqNo struct {
	Sequence int64
	Index    int
}

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
