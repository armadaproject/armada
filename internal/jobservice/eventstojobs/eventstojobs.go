package eventstojobs

import (
	"context"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
)

type EventsToJobService struct {
	queue                string
	jobsetid             string
	jobid                string
	apiConnection        client.ApiConnectionDetails
	jobServiceRepository repository.JobServiceRepository
}

func NewEventsToJobService(
	queue string,
	jobsetid string,
	jobid string,
	apiConnection client.ApiConnectionDetails,
	jobServiceRepository repository.JobServiceRepository) *EventsToJobService {
	return &EventsToJobService{
		queue:                queue,
		jobsetid:             jobsetid,
		jobid:                jobid,
		apiConnection:        apiConnection,
		jobServiceRepository: jobServiceRepository}
}

// TODO: This function will use redis and a pub/sub method.
// For now, we will use the api for streaming events and allow for clients to treat this as polling.
// This is not a production usecase yet.*jobservice.JobServiceResponse,
func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context) error {
	return client.WithEventClient(&eventToJobService.apiConnection, func(c api.EventClient) error {
		client.WatchJobSet(c, eventToJobService.queue, eventToJobService.jobsetid, true, true, context, func(state *domain.WatchContext, event api.Event) bool {
			eventMessage, err := api.Wrap(event)
			if err != nil {
				log.Error(err)
			}
			if !IsEventAJobResponse(*eventMessage) {
			} else {
				jobStatus, eventJobErr := EventsToJobResponse(*eventMessage)
				if eventJobErr != nil {
					// This can mean that the event type reported from server is unknown to the client
					log.Error(eventJobErr)
				}
				e := eventToJobService.jobServiceRepository.UpdateJobServiceDb(api.JobIdFromApiEvent(eventMessage), jobStatus)
				if e != nil {
					log.Error(e)
				}
			}
			// Active 
			return state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs()
		})
		return nil
	})
}
