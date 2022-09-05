package eventid

import (
	"fmt"

	"github.com/G-Research/armada/internal/pulsarutils"
	"github.com/apache/pulsar-client-go/pulsar"
)

// EventId uniquely identifies an event submitted to Pulsar.
// In particular, it combines the Pulsar message id with the index of the event.
type EventId struct {
	// Pulsar message id of the message containing the event.
	pulsarMessageId *pulsarutils.PulsarMessageId
	// index of the event referred to.
	// For example, the first event in a particular sequence would have index 0.
	index int
}

func New(pulsarMessageId pulsar.MessageID, index int) *EventId {
	return &EventId{
		pulsarMessageId: pulsarutils.FromMessageId(pulsarMessageId),
		index:           index,
	}
}

func (id *EventId) String() string {
	return fmt.Sprintf(
		"EventId{ledger: %d, entry: %d, partition: %d, batch: %d, event %d}",
		id.pulsarMessageId.LedgerID(),
		id.pulsarMessageId.EntryID(),
		id.pulsarMessageId.PartitionIdx(),
		id.pulsarMessageId.BatchIdx(),
		id.index,
	)
}

// Greater returns true if id occurred after other, or an error if the ids are not comparable
// (i.e., if they are from different partitions).
func (id *EventId) Greater(other *EventId) (bool, error) {
	isGreater, err := id.pulsarMessageId.Greater(other.pulsarMessageId)
	if err != nil {
		return false, err
	}
	if isGreater {
		return isGreater, nil
	}

	isEqual, err := id.pulsarMessageId.Equal(other.pulsarMessageId)
	if err != nil {
		return false, err
	}
	if isEqual {
		return id.index > other.index, nil
	}

	return false, nil
}

func (id *EventId) Equal(other *EventId) (bool, error) {
	isEqual, err := id.pulsarMessageId.Equal(other.pulsarMessageId)
	if err != nil {
		return false, err
	}
	if isEqual {
		return id.index == other.index, nil
	}
	return false, nil
}

func (id *EventId) GreaterEqual(other *EventId) (bool, error) {
	result, err := id.Equal(other)
	if err != nil {
		return false, err
	}
	if result {
		return true, nil
	}

	result, err = id.Greater(other)
	if err != nil {
		return false, err
	}
	if result {
		return true, nil
	}

	return false, nil
}
