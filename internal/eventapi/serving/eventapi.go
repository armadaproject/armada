package serving

import (
	ctx "context"
	"fmt"
	"github.com/G-Research/armada/internal/common/compress"
	"github.com/G-Research/armada/internal/eventapi"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/armadaevents"
	"github.com/gogo/protobuf/proto"
	"time"
)

type PostgresEventRepository struct {
	mapper              eventapi.JobsetMapper
	subscriptionManager *SubscriptionManager
	offsets             SequenceManager
}

func (r *PostgresEventRepository) CheckStreamExists(queue string, jobSetId string) (bool, error) {
	return true, nil
}

func (r *PostgresEventRepository) GetLastMessageId(queue, jobSetId string) (string, error) {
	id, err := r.mapper.Get(ctx.Background(), queue, jobSetId)
	if err != nil {
		return "", err
	}
	offset, _ := r.offsets.Get(id)
	return fmt.Sprint(offset), nil
}

func (r *PostgresEventRepository) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	jobsetId, err := r.mapper.Get(ctx.Background(), request.Queue, request.Id)
	if err != nil {
		return err
	}
	subscription := r.subscriptionManager.Subscribe(jobsetId, 0)
	defer r.subscriptionManager.Unsubscribe(subscription.SubscriptionId)
	decompressor, err := compress.NewZlibDecompressor()
	if err != nil {
		return err
	}
	for events := range subscription.Channel {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}
		for _, compressedEvent := range events {
			decompressedEvent, err := decompressor.Decompress(compressedEvent.Event)
			if err != nil {
				return err
			}
			dbEvent := &armadaevents.DatabaseEvent{}
			err = proto.Unmarshal(decompressedEvent, dbEvent)
			if err != nil {
				return err
			}
			apiEvents, err := ToApiMessage(dbEvent, request.Queue, request.Id)
			if err != nil {
				return err
			}
			for _, apiEvent := range apiEvents {
				stream.Send(&api.EventStreamMessage{
					Id:      "",
					Message: apiEvent,
				})
			}
		}
	}
	return nil
}

func ToApiMessage(dbEvent *armadaevents.DatabaseEvent, queue string, jobset string) ([]*api.EventMessage, error) {
	switch event := dbEvent.GetEvent().(type) {
	case *armadaevents.DatabaseEvent_EventSequence:
		event.EventSequence.Queue = queue
		event.EventSequence.JobSetName = jobset
		return fromEventSequence(event.EventSequence)
	case *armadaevents.DatabaseEvent_JobUtilisation:
		event.JobUtilisation.Queue = queue
		event.JobUtilisation.JobSetId = jobset
		return fromJobUtilisation(event.JobUtilisation)
	}
	return nil, nil
}

func fromEventSequence(es *armadaevents.EventSequence) ([]*api.EventMessage, error) {
	apiEvents := make([]*api.EventMessage, 0)
	var err error = nil
	var convertedEvents []*api.EventMessage = nil
	for _, event := range es.Events {
		switch esEvent := event.GetEvent().(type) {
		case *armadaevents.EventSequence_Event_SubmitJob:
			convertedEvents, err = FromLogSubmit(es.Queue, es.JobSetName, time.Now(), esEvent.SubmitJob)
		case *armadaevents.EventSequence_Event_CancelledJob:
			convertedEvents, err = FromLogCancelled(es.UserId, es.Queue, es.JobSetName, time.Now(), esEvent.CancelledJob)
		case *armadaevents.EventSequence_Event_CancelJob:
			convertedEvents, err = FromLogCancelling(es.UserId, es.Queue, es.JobSetName, time.Now(), esEvent.CancelJob)
		case *armadaevents.EventSequence_Event_ReprioritiseJob:
			convertedEvents, err = FromLogReprioritizing(es.UserId, es.Queue, es.JobSetName, time.Now(), esEvent.ReprioritiseJob)
		case *armadaevents.EventSequence_Event_ReprioritisedJob:
			convertedEvents, err = FromLogReprioritised(es.UserId, es.Queue, es.JobSetName, time.Now(), esEvent.ReprioritisedJob)
		case *armadaevents.EventSequence_Event_JobDuplicateDetected:
			convertedEvents, err = FromLogDuplicateDetected(es.Queue, es.JobSetName, time.Now(), esEvent.JobDuplicateDetected)
		case *armadaevents.EventSequence_Event_JobRunLeased:
			convertedEvents, err = FromLogJobRunLeased(es.Queue, es.JobSetName, time.Now(), esEvent.JobRunLeased)
		case *armadaevents.EventSequence_Event_JobRunErrors:
			convertedEvents, err = FromJobRunErrors(es.Queue, es.JobSetName, time.Now(), esEvent.JobRunErrors)
		case *armadaevents.EventSequence_Event_JobErrors:
			convertedEvents, err = FromJobErrors(es.Queue, es.JobSetName, time.Now(), esEvent.JobErrors)
		case *armadaevents.EventSequence_Event_JobRunRunning:
			convertedEvents, err = FromJobRunRunning(es.Queue, es.JobSetName, time.Now(), esEvent.JobRunRunning)
		case *armadaevents.EventSequence_Event_JobRunAssigned:
			convertedEvents, err = FromJobRunAssigned(es.Queue, es.JobSetName, time.Now(), esEvent.JobRunAssigned)
		}
		if err != nil {
			//TODO: would it be better to log a warning and continue?
			return nil, err
		}
		apiEvents = append(apiEvents, convertedEvents...)
	}
	return apiEvents, nil
}

func fromJobUtilisation(dbEvent *armadaevents.JobUtilisationEvent) ([]*api.EventMessage, error) {
	return []*api.EventMessage{{Events: &api.EventMessage_Utilisation{
		Utilisation: &api.JobUtilisationEvent{
			JobId:                 dbEvent.JobId,
			Queue:                 dbEvent.Queue,
			Created:               dbEvent.Created,
			ClusterId:             dbEvent.ClusterId,
			KubernetesId:          dbEvent.KubernetesId,
			MaxResourcesForPeriod: dbEvent.MaxResourcesForPeriod,
			NodeName:              dbEvent.NodeName,
			PodNumber:             dbEvent.PodNumber,
			PodName:               dbEvent.PodName,
			PodNamespace:          dbEvent.PodNamespace,
			TotalCumulativeUsage:  dbEvent.TotalCumulativeUsage,
		},
	}}}, nil
}
