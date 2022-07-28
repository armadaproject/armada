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
		jobServiceRepository: jobServiceRepository,
	}
}

func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context) error {

	err := eventToJobService.StreamCommon(&eventToJobService.jobServiceConfig.ApiConnection, context)
	if err != nil {
		log.Warnf("Error found from StreamCommon: %v", err)
	}
	log.Info("Ending StreamCommon")
	return nil
}
func (eventToJobService *EventsToJobService) StreamCommon(clientConnect *client.ApiConnectionDetails, ctx context.Context) error {
	var fromMessageId string
	err := client.WithEventClient(clientConnect, func(c api.EventClient) error {
		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:             eventToJobService.jobsetid,
			Queue:          eventToJobService.queue,
			Watch:          true,
			FromMessageId:  fromMessageId,
			ErrorIfMissing: false,
		})
		eventToJobService.jobServiceRepository.SubscribeJobSet(eventToJobService.jobsetid)
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
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(60 * time.Second):
				if eventToJobService.jobServiceRepository.IsJobSetSubscribed(eventToJobService.jobsetid) {
					log.Infof("JobSet %s is unsubscribed", eventToJobService.jobsetid)
					return nil
				}
			default:
			}
			fromMessageId = msg.GetId()
			currentJobId := api.JobIdFromApiEvent(msg.Message)
			jobStatus := EventsToJobResponse(*msg.Message)
			if jobStatus != nil {
				jobTable := repository.NewJobTable(eventToJobService.queue, eventToJobService.jobsetid, currentJobId, *jobStatus)
				updateErr := eventToJobService.jobServiceRepository.UpdateJobServiceDb(currentJobId, jobTable)
				if updateErr != nil {
					log.Error(updateErr)
				}
			}
		}
	})
	return err
}
