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
	// This corresponds to the amount of time since the job-set has been updated.
	// If no new events come from the job-set, then the timer allows automatic unsubscribing.
	duration := time.Duration(eventToJobService.jobServiceConfig.SubscribeJobSetTime) * time.Second
	timeOut := time.NewTimer(duration)
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
			case <-timeOut.C:
				err := eventToJobService.jobServiceRepository.UnSubscribeJobSet(eventToJobService.jobsetid)
				if err != nil {
					log.Errorf("Unsubscribe errored with %v", err)
					return err
				}
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
				// Every new event should reset timer.
				timeOut.Reset(duration)
			}
		}
	})
	return err
}
