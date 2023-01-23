package reporter

import (
	"context"
	"fmt"

	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/eventutil"
	"github.com/armadaproject/armada/internal/executor/configuration"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/armadaevents"
	"github.com/armadaproject/armada/pkg/executorapi"
)

type EventSender interface {
	SendEvents(events []EventMessage) error
}

type ExecutorApiEventSender struct {
	eventClient  executorapi.ExecutorApiClient
	clientConfig configuration.ClientConfiguration
}

func NewExecutorApiEventSender(
	executorApiClient executorapi.ExecutorApiClient,
	clientConfig configuration.ClientConfiguration,
) *ExecutorApiEventSender {
	return &ExecutorApiEventSender{
		eventClient:  executorApiClient,
		clientConfig: clientConfig,
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
		runId, err := armadaevents.ProtoUuidFromUuidString(e.JobRunId)
		if err != nil {
			return fmt.Errorf("Failed to convert uuid string %s to uuid because %s", e.JobRunId, err)
		}
		populateRunId(sequence, runId)
		sequences = append(sequences, sequence)
	}
	sequences = eventutil.CompactEventSequences(sequences)
	sequences, err := eventutil.LimitSequencesByteSize(sequences, eventSender.clientConfig.MaxMessageSizeBytes, true)
	if err != nil {
		return err
	}

	_, err = eventSender.eventClient.ReportEvents(context.Background(), &executorapi.EventList{Events: sequences})
	return err
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
			//TODO set preemptive run id correctly
			runEvent.JobRunPreempted.PreemptedRunId = jobRunId
		case *armadaevents.EventSequence_Event_ResourceUtilisation:
			runEvent.ResourceUtilisation.JobId = jobRunId
		default:
			log.Warnf("unexpected event type %T- failed to populate run id", event)
		}
	}
}

type LegacyApiEventSender struct {
	eventClient api.EventClient
}

func NewLegacyApiEventSender(eventClient api.EventClient) *LegacyApiEventSender {
	return &LegacyApiEventSender{
		eventClient: eventClient,
	}
}

func (eventSender *LegacyApiEventSender) SendEvents(events []EventMessage) error {
	var eventMessages []*api.EventMessage
	for _, e := range events {
		m, err := api.Wrap(e.Event)
		eventMessages = append(eventMessages, m)
		if err != nil {
			return err
		}
		log.Debugf("Reporting event %+v", m)
	}
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	_, err := eventSender.eventClient.ReportMultiple(ctx, &api.EventList{eventMessages})
	return err
}
