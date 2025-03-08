package reporter

import (
	"google.golang.org/protobuf/proto"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type EventSender interface {
	SendEvents(events []EventMessage) error
}

const eventListOverheadBytes int = 50

type ExecutorApiEventSender struct {
	eventClient    executorapi.ExecutorApiClient
	maxMessageSize int
}

func NewExecutorApiEventSender(
	executorApiClient executorapi.ExecutorApiClient,
	maxMessageSize int,
) *ExecutorApiEventSender {
	return &ExecutorApiEventSender{
		eventClient:    executorApiClient,
		maxMessageSize: maxMessageSize,
	}
}

func (eventSender *ExecutorApiEventSender) SendEvents(events []EventMessage) error {
	sequences := make([]*armadaevents.EventSequence, 0, len(events))
	for _, e := range events {
		sequences = append(sequences, e.Event)
	}
	sequences = eventutil.CompactEventSequences(sequences)
	if len(sequences) <= 0 {
		return nil
	}
	eventLists, err := splitIntoEventListWithByteLimit(sequences, eventSender.maxMessageSize)
	if err != nil {
		return err
	}

	for _, eventList := range eventLists {
		_, err = eventSender.eventClient.ReportEvents(armadacontext.Background(), eventList)
		if err != nil {
			return err
		}
	}

	return err
}

func splitIntoEventListWithByteLimit(sequences []*armadaevents.EventSequence, maxEventListSizeBytes int) ([]*executorapi.EventList, error) {
	sequences, err := eventutil.LimitSequencesByteSize(sequences, uint(maxEventListSizeBytes-eventListOverheadBytes), true)
	if err != nil {
		return nil, err
	}

	result := []*executorapi.EventList{}
	currentEventList := &executorapi.EventList{}
	currentEventList.Events = make([]*armadaevents.EventSequence, 0)
	currentEventListSize := proto.Size(currentEventList)
	result = append(result, currentEventList)

	for _, sequence := range sequences {
		sequenceSizeBytes := proto.Size(sequence)
		if sequenceSizeBytes+currentEventListSize < maxEventListSizeBytes {
			currentEventList.Events = append(currentEventList.Events, sequence)
			currentEventListSize = currentEventListSize + sequenceSizeBytes
		} else {
			newEventList := &executorapi.EventList{}
			newEventList.Events = make([]*armadaevents.EventSequence, 0)
			newEventList.Events = append(newEventList.Events, sequence)

			currentEventList = newEventList
			currentEventListSize = proto.Size(currentEventList)
			result = append(result, currentEventList)
		}
	}

	return result, nil
}
