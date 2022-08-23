package eventid

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/G-Research/armada/internal/pulsarutils"
)

// EventId identifies a particular event within an event sequence.
// Because event sequences and Pulsar messages map one-to-one, the event sequence is identified by a Pulsar message id.
// The specific event within that sequence is identified by the index of the event within the sequence.
//
// This type satisfies the pulsar.MessageID interface.
type EventId struct {
	messageId  pulsar.MessageID
	sequenceId int32
}

func New(messageId pulsar.MessageID, sequenceId int32) *EventId {
	return &EventId{
		messageId:  messageId,
		sequenceId: sequenceId,
	}
}

func (id *EventId) MessageId() pulsar.MessageID {
	return pulsarutils.New(id.LedgerID(), id.EntryID(), id.PartitionIdx(), id.BatchIdx())
}

func (id *EventId) LedgerID() int64 {
	return id.messageId.LedgerID()
}

func (id *EventId) EntryID() int64 {
	return id.messageId.EntryID()
}

func (id *EventId) PartitionIdx() int32 {
	return id.messageId.PartitionIdx()
}

func (id *EventId) BatchIdx() int32 {
	return id.messageId.BatchIdx()
}

func (id *EventId) Serialize() []byte {
	panic("not implemented")
}

func (id *EventId) SequenceId() int32 {
	return id.sequenceId
}

// Greater returns true if id occurred after other, or an error if the message ids are not comparable
// (i.e., if they are from different partitions).
func (id *EventId) Greater(other *EventId) (bool, error) {
	idx := pulsarutils.FromMessageId(id.MessageId())
	otherx := pulsarutils.FromMessageId(other.MessageId())

	sequenceIsEqual, err := idx.Equal(otherx)
	if err != nil {
		return false, err
	}
	if sequenceIsEqual {
		return id.sequenceId > other.sequenceId, nil
	}

	sequenceIsGreater, err := idx.Greater(otherx)
	if err != nil {
		return false, err
	}
	return sequenceIsGreater, nil
}

func (id *EventId) Equal(other *EventId) (bool, error) {
	idx := pulsarutils.FromMessageId(id.MessageId())
	otherx := pulsarutils.FromMessageId(other.MessageId())

	isEqual, err := idx.Equal(otherx)
	if err != nil {
		return false, err
	}
	if isEqual {
		return id.sequenceId == other.sequenceId, nil
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
