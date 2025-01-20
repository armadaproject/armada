package jobsetevents

import (
	"github.com/apache/pulsar-client-go/pulsar"

	"github.com/armadaproject/armada/internal/common/eventutil"
	commonmetrics "github.com/armadaproject/armada/internal/common/ingest/metrics"
	"github.com/armadaproject/armada/internal/common/ingest/utils"
	log "github.com/armadaproject/armada/internal/common/logging"
	protoutil "github.com/armadaproject/armada/internal/common/proto"
	"github.com/armadaproject/armada/pkg/armadaevents"
)

func MessageUnmarshaller(msg pulsar.ConsumerMessage, metrics *commonmetrics.Metrics) *utils.EventsWithIds[*armadaevents.EventSequence] {
	sequences := make([]*armadaevents.EventSequence, 0, 1)
	messageIds := make([]pulsar.MessageID, 0, 1)

	// Record the messageId- we need to record all message Ids, even if the event they contain is invalid
	// As they must be acked at the end
	messageIds = append(messageIds, msg.ID())

	// Try and unmarshall the proto
	es, err := eventutil.UnmarshalEventSequence(msg.Payload())
	if err != nil {
		metrics.RecordPulsarMessageError(commonmetrics.PulsarMessageErrorDeserialization)
		log.WithError(err).Warnf("Could not unmarshal proto for msg %s", msg.ID())
	} else {
		// Fill in time if it is not set
		// TODO - once created is set everywhere we can remove this
		for _, event := range es.Events {
			if event.GetCreated() == nil {
				event.Created = protoutil.ToTimestamp(msg.PublishTime())
			}
		}
		sequences = append(sequences, es)
	}
	return &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: sequences, MessageIds: messageIds,
	}
}

func BatchMetricPublisher(metrics *commonmetrics.Metrics, batch *utils.EventsWithIds[*armadaevents.EventSequence]) {
	numberOfEvents := 0
	countOfEventsByType := map[string]int{}
	for _, event := range batch.Events {
		numberOfEvents += len(event.Events)
		for _, e := range event.Events {
			typeString := e.GetEventName()
			countOfEventsByType[typeString] = countOfEventsByType[typeString] + 1

			// Record the event sequence as processed here.  This is technically not true as we haven't finished
			// processing yet but it saves us having to pass all these details down the pipeline and as the
			// pipeline is single threaded, the error here should be inconsequential.
			metrics.RecordEventSequenceProcessed(event.Queue, typeString)
		}
	}
	log.Infof("Batch being processed contains %d event messages and %d events of type %v", len(batch.MessageIds), numberOfEvents, countOfEventsByType)
}

func EventCounter(events *utils.EventsWithIds[*armadaevents.EventSequence]) int {
	totalEvents := 0
	for _, event := range events.Events {
		totalEvents += len(event.Events)
	}
	return totalEvents
}

func BatchMerger(nestedSequences []*utils.EventsWithIds[*armadaevents.EventSequence]) *utils.EventsWithIds[*armadaevents.EventSequence] {
	combinedSequences := make([]*armadaevents.EventSequence, 0)
	messageIds := []pulsar.MessageID{}
	for _, eventsWithIds := range nestedSequences {
		combinedSequences = append(combinedSequences, eventsWithIds.Events...)
		messageIds = append(messageIds, eventsWithIds.MessageIds...)
	}
	return &utils.EventsWithIds[*armadaevents.EventSequence]{
		Events: combinedSequences, MessageIds: messageIds,
	}
}
