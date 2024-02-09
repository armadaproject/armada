package reporter

import (
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type EventSender interface {
	SendEvents(events []EventMessage) error
}

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
		message, err := api.Wrap(e.Event)
		if err != nil {
			return err
		}
		log.Debugf("Reporting event %+v", message)
		sequence, err := eventutil.EventSequenceFromApiEvent(message)
		if err != nil {
			return err
		}
		sequence.Events = filterJobRunEvents(sequence.Events)
		runId, err := armadaevents.ProtoUuidFromUuidString(e.JobRunId)
		if err != nil {
			return errors.Errorf("failed to convert uuid string %s to uuid because %s", e.JobRunId, err)
		}
		populateRunId(sequence, runId)
		sequences = append(sequences, sequence)
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
	sequences, err := eventutil.LimitSequencesByteSize(sequences, uint(maxEventListSizeBytes), true)
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

func filterJobRunEvents(events []*armadaevents.EventSequence_Event) []*armadaevents.EventSequence_Event {
	result := make([]*armadaevents.EventSequence_Event, 0, len(events))
	for _, event := range events {
		switch typed := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_JobSucceeded:
			continue
		case *armadaevents.EventSequence_Event_JobErrors:
			continue
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
			result = append(result, event)
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
			result = append(result, event)
		default:
			log.Warnf("unexpected event type %T- filtering it out", typed)
		}
	}
	return result
}

func populateRunId(eventSequence *armadaevents.EventSequence, jobRunId *armadaevents.Uuid) {
	for _, event := range eventSequence.Events {
		switch runEvent := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			runEvent.JobRunAssigned.RunId = jobRunId
		case *armadaevents.EventSequence_Event_JobRunRunning:
			runEvent.JobRunRunning.RunId = jobRunId
		case *armadaevents.EventSequence_Event_JobRunSucceeded:
			runEvent.JobRunSucceeded.RunId = jobRunId
		case *armadaevents.EventSequence_Event_JobRunErrors:
			runEvent.JobRunErrors.RunId = jobRunId
		case *armadaevents.EventSequence_Event_JobRunPreempted:
			runEvent.JobRunPreempted.PreemptedRunId = jobRunId
		case *armadaevents.EventSequence_Event_StandaloneIngressInfo:
			runEvent.StandaloneIngressInfo.RunId = jobRunId
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
			runEvent.ResourceUtilisation.RunId = jobRunId
		default:
			log.Warnf("unexpected event type %T- failed to populate run id", runEvent)
		}
	}
}
