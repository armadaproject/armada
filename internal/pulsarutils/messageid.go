package pulsarutils

import (
	"fmt"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/G-Research/armada/internal/common/armadaerrors"
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
func (id *PulsarMessageId) Greater(other *PulsarMessageId) (bool, error) {
	if id.partitionIdx != other.partitionIdx {
		err := &armadaerrors.ErrInvalidArgument{
			Name:    "other.partitionIdx",
			Value:   other.partitionIdx,
			Message: fmt.Sprintf("expected %d; messages from different partitions are not comparable", id.partitionIdx),
		}
		return false, errors.WithStack(err)
	}

	if id.ledgerID != other.ledgerID {
		return id.ledgerID > other.ledgerID, nil
	}

	if id.entryID != other.entryID {
		return id.entryID > other.entryID, nil
	}

	return id.batchIdx > other.batchIdx, nil
}

func (id *PulsarMessageId) Equal(other *PulsarMessageId) (bool, error) {
	if id.partitionIdx != other.partitionIdx {
		err := &armadaerrors.ErrInvalidArgument{
			Name:    "other.partitionIdx",
			Value:   other.partitionIdx,
			Message: fmt.Sprintf("expected %d; messages from different partitions are not comparable", id.partitionIdx),
		}
		return false, errors.WithStack(err)
	}

	return id.ledgerID == other.ledgerID &&
		id.entryID == other.entryID &&
		id.batchIdx == other.batchIdx, nil
}

func (id *PulsarMessageId) GreaterEqual(other *PulsarMessageId) (bool, error) {
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
