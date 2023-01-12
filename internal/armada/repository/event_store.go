package repository

import (
	"context"
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/G-Research/armada/internal/common/eventutil"
	"github.com/G-Research/armada/internal/common/pulsarutils"
	"github.com/G-Research/armada/pkg/api"
)

type EventStore interface {
	ReportEvents(message []*api.EventMessage) error
}

type TestEventStore struct {
	ReceivedEvents []*api.EventMessage
}

func (es *TestEventStore) ReportEvents(message []*api.EventMessage) error {
	es.ReceivedEvents = append(es.ReceivedEvents, message...)
	return nil
}

type StreamEventStore struct {
	Producer              pulsar.Producer
	MaxAllowedMessageSize int
}

func NewEventStore(producer pulsar.Producer, maxAllowedMessageSize int) *StreamEventStore {
	return &StreamEventStore{
		Producer: producer, MaxAllowedMessageSize: maxAllowedMessageSize,
	}
}

func (n *StreamEventStore) ReportEvents(apiEvents []*api.EventMessage) error {
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
	sequences, err = eventutil.LimitSequencesByteSize(sequences, int(n.MaxAllowedMessageSize), true)
	if err != nil {
		return err
	}
	return pulsarutils.PublishSequences(context.Background(), n.Producer, sequences)
}
