package controlplaneevents

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/armadaproject/armada/internal/common/eventutil"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"

)

func MessageUnmarshaller(msg pulsar.ConsumerMessage, metrics *commonmetrics.Metrics) *utils.EventsWithIds[*controlplaneevents.Event] {
	events := make([]*controlplaneevents.Event, 0, 1)
	messageIds := make([]pulsar.MessageID, 0, 1)

	messageIds = append(messageIds, msg.ID())

	// Try and unmarshall the proto
	event, err := eventutil.UnmarshalControlPlaneEvent(msg.Payload())
	if err != nil {
		metrics.RecordPulsarMessageError(commonmetrics.PulsarMessageErrorDeserialization)
		log.WithError(err).Warnf("Could not unmarshal proto for msg %s", msg.ID())
	} else {
		events = append(events, event)
	}
	return &utils.EventsWithIds[*controlplaneevents.Event]{
		Events: events, MessageIds: messageIds,
	}
}

func BatchMerger(nestedSequences []*utils.EventsWithIds[*controlplaneevents.Event]) *utils.EventsWithIds[*controlplaneevents.Event] {
	combinedSequences := make([]*controlplaneevents.Event, 0)
	messageIds := []pulsar.MessageID{}
	for _, eventsWithIds := range nestedSequences {
		combinedSequences = append(combinedSequences, eventsWithIds.Events...)
		messageIds = append(messageIds, eventsWithIds.MessageIds...)
	}
	return &utils.EventsWithIds[*controlplaneevents.Event]{
		Events: combinedSequences, MessageIds: messageIds,
	}
}

func BatchMetricPublisher(metrics *commonmetrics.Metrics, batch *utils.EventsWithIds[*controlplaneevents.Event]) {
	countOfEventsByType := map[string]int{}
	for _, event := range batch.Events {
		typeString := event.GetEventName()
		countOfEventsByType[typeString] = countOfEventsByType[typeString] + 1
		metrics.RecordControlPlaneEventProcessed(typeString)
	}
	log.Infof("Batch being processed contains a ControlPlane event of type %v and associated message ID", countOfEventsByType)
}

func EventCounter(events *utils.EventsWithIds[*controlplaneevents.Event]) int {
	return len(events.Events)
}
