package eventstojobs

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

type EventsToJobService struct {
	queue                string
	jobsetid             string
	jobid                string
	jobServiceConfig     *configuration.JobServiceConfiguration
	jobServiceRepository repository.JobServiceRepository
}

func NewEventsToJobService(
	queue string,
	jobsetid string,
	jobid string,
	jobServiceConfig *configuration.JobServiceConfiguration,
	jobServiceRepository repository.JobServiceRepository) *EventsToJobService {
	return &EventsToJobService{
		queue:                queue,
		jobsetid:             jobsetid,
		jobid:                jobid,
		jobServiceConfig:     jobServiceConfig,
		jobServiceRepository: jobServiceRepository}
}

func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context) error {

	return eventToJobService.StreamCommon(&eventToJobService.jobServiceConfig.ApiConnection, context)
}
func (eventToJobService *EventsToJobService) StreamCommon(clientConnect *client.ApiConnectionDetails, ctx context.Context) error {
	var fromMessageId string
	// How long we want our subscribing of a job-set to last.
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(eventToJobService.jobServiceConfig.SubscribeJobSetTime)*time.Second)
	defer cancel()
	err := client.WithEventClient(clientConnect, func(c api.EventClient) error {
		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:             eventToJobService.jobsetid,
			Queue:          eventToJobService.queue,
			Watch:          true,
			FromMessageId:  fromMessageId,
			ErrorIfMissing: false,
		})
		if err != nil {
			log.Error(err)
			return err
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Error(err)
				return err
			}
			fromMessageId = msg.GetId()
			currentJobId := api.JobIdFromApiEvent(msg.Message)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ctxTimeout.Done():
				log.Infof("Unsubscribing from %s", eventToJobService.jobsetid)
				return eventToJobService.jobServiceRepository.UnSubscribeJobSet(eventToJobService.jobsetid)
			default:
			}
			if IsEventAJobResponse(*msg.Message) {
				jobStatus, eventJobErr := EventsToJobResponse(*msg.Message)
				if eventJobErr != nil {
					// This can mean that the event type reported from server is unknown to the client
					log.Error(eventJobErr)
				}
				jobTable := repository.NewJobTable(eventToJobService.queue, eventToJobService.jobsetid, eventToJobService.jobid, *jobStatus)
				updateErr := eventToJobService.jobServiceRepository.UpdateJobServiceDb(currentJobId, jobTable)
				if updateErr != nil {
					log.Error(updateErr)
				}
			}
		}
	})
	return err
}
