package eventutil

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

// PulsarToChannel is a service for receiving messages from Pulsar and forwarding those on C.
type SequenceFromMessage struct {
	In  chan pulsar.Message
	Out chan *EventSequenceWithMessageIds
}

// EventSequenceWithMessageIds bundles an event sequence with
// all the ids of all Pulsar messages that were consumed to produce it.
type EventSequenceWithMessageIds struct {
	Sequence   *armadaevents.EventSequence
	MessageIds []pulsar.MessageID
}

func NewSequenceFromMessage(in chan pulsar.Message) *SequenceFromMessage {
	return &SequenceFromMessage{
		In:  in,
		Out: make(chan *EventSequenceWithMessageIds),
	}
}

func (srv *SequenceFromMessage) Run(ctx *armadacontext.Context) error {
	log := ctxlogrus.Extract(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg := <-srv.In:
			if msg == nil {
				break
			}
			sequence, err := UnmarshalEventSequence(ctx, msg.Payload())
			if err != nil {
				logging.WithStacktrace(log, err).WithField("messageid", msg.ID()).Error("failed to unmarshal event sequence")
				break
			}

			sequenceWithMessageIds := &EventSequenceWithMessageIds{
				Sequence:   sequence,
				MessageIds: []pulsar.MessageID{msg.ID()},
			}
			select {
			case <-ctx.Done():
			case srv.Out <- sequenceWithMessageIds:
			}
		}
	}
}

// SequenceCompacter reads sequences and produces compacted sequences.
// Compacted sequences are created by combining events in sequences with the
type SequenceCompacter struct {
	In  chan *EventSequenceWithMessageIds
	Out chan *EventSequenceWithMessageIds
	// Buffer messages for at most this long before forwarding on the outgoing channel.
	Interval time.Duration
	// Max number of events to buffer.
	MaxEvents int
	// Buffer of events to be compacted and sent.
	buffer []*EventSequenceWithMessageIds
	// Number of events collected so far.
	numEvents int
}

func NewSequenceCompacter(in chan *EventSequenceWithMessageIds) *SequenceCompacter {
	return &SequenceCompacter{
		In:        in,
		Out:       make(chan *EventSequenceWithMessageIds),
		Interval:  5 * time.Second,
		MaxEvents: 10000,
	}
}

func (srv *SequenceCompacter) Run(ctx *armadacontext.Context) error {
	ticker := time.NewTicker(srv.Interval)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			err := srv.compactAndSend(ctx)
			if err != nil {
				return err
			}
		case sequenceWithIds := <-srv.In:
			if sequenceWithIds == nil || sequenceWithIds.Sequence == nil {
				break
			}
			srv.buffer = append(srv.buffer, sequenceWithIds)
			srv.numEvents += len(sequenceWithIds.Sequence.Events)
			if srv.numEvents > srv.MaxEvents {
				err := srv.compactAndSend(ctx)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (srv *SequenceCompacter) compactAndSend(ctx *armadacontext.Context) error {
	if len(srv.buffer) == 0 {
		return nil
	}

	// Compact the event sequences.
	// Note that we can't be sure of the number of message ids.
	messageIds := make([]pulsar.MessageID, 0, len(srv.buffer))
	sequences := make([]*armadaevents.EventSequence, len(srv.buffer))
	for i, sequenceWithIds := range srv.buffer {
		messageIds = append(messageIds, sequenceWithIds.MessageIds...)
		sequences[i] = sequenceWithIds.Sequence
	}
	sequences = CompactEventSequences(sequences)

	for i, sequence := range sequences {
		sequenceWithIds := &EventSequenceWithMessageIds{
			Sequence: sequence,
		}

		// Add all message ids to the last sequence to be produced.
		// To avoid later ack'ing messages the data of which has not yet been processed.
		if i == len(sequences)-1 {
			sequenceWithIds.MessageIds = messageIds
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case srv.Out <- sequenceWithIds:
		}
	}

	// Empty the buffer.
	srv.buffer = nil
	srv.numEvents = 0

	return nil
}

// EventFilter calls filter once for each event,
// and events for which filter returns false are discarded.
type EventFilter struct {
	In  chan *EventSequenceWithMessageIds
	Out chan *EventSequenceWithMessageIds
	// Filter function. Discard on returning false.
	filter func(*armadaevents.EventSequence_Event) bool
}

func NewEventFilter(in chan *EventSequenceWithMessageIds, filter func(*armadaevents.EventSequence_Event) bool) *EventFilter {
	return &EventFilter{
		In:     in,
		Out:    make(chan *EventSequenceWithMessageIds),
		filter: filter,
	}
}

func (srv *EventFilter) Run(ctx *armadacontext.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sequenceWithIds := <-srv.In:
			if sequenceWithIds == nil {
				break
			}
			events := make([]*armadaevents.EventSequence_Event, 0, len(sequenceWithIds.Sequence.Events))
			for _, event := range sequenceWithIds.Sequence.Events {
				if srv.filter(event) {
					events = append(events, event)
				}
			}
			sequenceWithIds.Sequence.Events = events

			select {
			case <-ctx.Done():
			case srv.Out <- sequenceWithIds:
			}
		}
	}
}
