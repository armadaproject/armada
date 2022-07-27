package eventstojobs

import (
	"context"
	"fmt"

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
	unsubscribeMessage := make(chan *string)
	// This corresponds to the amount of time since the job-set has been updated.
	// If no new events come from the job-set, then the timer allows automatic unsubscribing.
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
			fromMessageId = msg.GetId()
			currentJobId := api.JobIdFromApiEvent(msg.Message)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-unsubscribeMessage:
				log.Infof("Ending subscription of %s", eventToJobService.jobsetid)
				return nil
			default:
			}
			jobStatus := EventsToJobResponse(*msg.Message)
			if jobStatus != nil {
				jobTable := repository.NewJobTable(eventToJobService.queue, eventToJobService.jobsetid, eventToJobService.jobid, *jobStatus)
				updateErr := eventToJobService.jobServiceRepository.UpdateJobServiceDb(currentJobId, jobTable)
				if updateErr != nil {
					log.Error(updateErr)
				}
			}
			log.Info("Checking Unsubscribe function")
			if !eventToJobService.jobServiceRepository.IsJobSetSubscribed(eventToJobService.jobsetid) {
				subscribeMessage := fmt.Sprintf("Unsubscribe to %s", eventToJobService.jobsetid)
				log.Info(subscribeMessage)
				unsubscribeMessage <- &subscribeMessage
			}
		}
	})
	return err
}
