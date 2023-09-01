package repository

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/common/pulsarutils"
	"github.com/armadaproject/armada/internal/common/schedulers"
	"github.com/armadaproject/armada/pkg/api"
)

type EventStore interface {
	ReportEvents(*armadacontext.ArmadaContext, []*api.EventMessage) error
}

type TestEventStore struct {
	ReceivedEvents []*api.EventMessage
}

func (es *TestEventStore) ReportEvents(_ *armadacontext.ArmadaContext, message []*api.EventMessage) error {
	es.ReceivedEvents = append(es.ReceivedEvents, message...)
	return nil
}

type StreamEventStore struct {
	Producer              pulsar.Producer
	MaxAllowedMessageSize uint
}

func NewEventStore(producer pulsar.Producer, maxAllowedMessageSize uint) *StreamEventStore {
	return &StreamEventStore{
		Producer: producer, MaxAllowedMessageSize: maxAllowedMessageSize,
	}
}

func (n *StreamEventStore) ReportEvents(ctx *armadacontext.ArmadaContext, apiEvents []*api.EventMessage) error {
	if len(apiEvents) == 0 {
		return nil
	}

	// Because (queue, userId, jobSetId) may differ between events,
	// several sequences may be necessary.
	sequences, err := eventutil.EventSequencesFromApiEvents(apiEvents)
	if err != nil {
		return err
	}
	if len(sequences) == 0 {
		return nil
	}

	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err = eventutil.LimitSequencesByteSize(sequences, n.MaxAllowedMessageSize, true)
	if err != nil {
		return err
	}
	return pulsarutils.PublishSequences(ctx, n.Producer, sequences, schedulers.Legacy)
}
