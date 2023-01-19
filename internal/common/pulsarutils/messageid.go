package pulsarutils

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

// PulsarMessageId implements the pulsar.MessageID interface (which uniquely identifies a Pulsar message).
// We need this since the pulsar client library does not export a MessageID implementation.
// For PulsarMessageId, we provide, e.g., comparison functions.
type PulsarMessageId struct {
	ledgerID     int64
	entryID      int64
	partitionIdx int32
	batchIdx     int32
}

func New(ledgerID, entryID int64, partitionIdx, batchIdx int32) *PulsarMessageId {
	return &PulsarMessageId{
		ledgerID:     ledgerID,
		entryID:      entryID,
		partitionIdx: partitionIdx,
		batchIdx:     batchIdx,
	}
}

func (id *PulsarMessageId) String() string {
	return fmt.Sprintf(
		"PulsarMessageId{ledger: %d, entry: %d, partition: %d, batch: %d}",
		id.ledgerID, id.entryID, id.partitionIdx, id.batchIdx,
	)
}

// FromMessageId converts a pulsar.MessageID interface type to a *PulsarMessageId,
// which can be used, e.g., for comparison.
func FromMessageId(id pulsar.MessageID) *PulsarMessageId {
	return &PulsarMessageId{
		ledgerID:     id.LedgerID(),
		entryID:      id.EntryID(),
		partitionIdx: id.PartitionIdx(),
		batchIdx:     id.BatchIdx(),
	}
}

func (id *PulsarMessageId) LedgerID() int64 {
	return id.ledgerID
}

func (id *PulsarMessageId) EntryID() int64 {
	return id.entryID
}

func (id *PulsarMessageId) PartitionIdx() int32 {
	return id.partitionIdx
}

func (id *PulsarMessageId) BatchIdx() int32 {
	return id.batchIdx
}

func (id *PulsarMessageId) Serialize() []byte {
	panic("not implemented")
}

// Greater returns true if id occurred after other, or an error if the message ids are not comparable
// (i.e., if they are from different partitions).
func (id *PulsarMessageId) Greater(other pulsar.MessageID) (bool, error) {
	if id.partitionIdx != other.PartitionIdx() {
		err := &armadaerrors.ErrInvalidArgument{
			Name:    "other.partitionIdx",
			Value:   other.PartitionIdx(),
			Message: fmt.Sprintf("expected %d, but got %d; messages from different partitions are not comparable", id.partitionIdx, other.PartitionIdx()),
		}
		return false, errors.WithStack(err)
	}

	if id.ledgerID != other.LedgerID() {
		return id.ledgerID > other.LedgerID(), nil
	}

	if id.entryID != other.EntryID() {
		return id.entryID > other.EntryID(), nil
	}

	return id.batchIdx > other.BatchIdx(), nil
}

func (id *PulsarMessageId) Equal(other pulsar.MessageID) (bool, error) {
	if id.partitionIdx != other.PartitionIdx() {
		err := &armadaerrors.ErrInvalidArgument{
			Name:    "other.partitionIdx",
			Value:   other.PartitionIdx(),
			Message: fmt.Sprintf("expected %d; messages from different partitions are not comparable", id.partitionIdx),
		}
		return false, errors.WithStack(err)
	}

	return id.ledgerID == other.LedgerID() &&
		id.entryID == other.EntryID() &&
		id.batchIdx == other.BatchIdx(), nil
}

func (id *PulsarMessageId) GreaterEqual(other pulsar.MessageID) (bool, error) {
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
