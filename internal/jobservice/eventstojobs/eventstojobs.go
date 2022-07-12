package eventstojobs

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"github.com/G-Research/armada/pkg/client/domain"
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
	return client.WithEventClient(&eventToJobService.jobServiceConfig.ApiConnection, func(c api.EventClient) error {
		client.WatchJobSet(c, eventToJobService.queue, eventToJobService.jobsetid, true, true, context, func(state *domain.WatchContext, event api.Event) bool {
			duration := time.Duration(eventToJobService.jobServiceConfig.SubscribeJobSetTime) * time.Second
			t1 := time.NewTimer(duration)
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
			// Case 1: End if we hit our configurable timeout for subscription.
			go func() bool {
				<-t1.C
				fmt.Println("Timer expired")
				return true
			}()

			// Case 2 for exiting: All jobs are finished in job-set
			return state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs()
		})
		return nil
	})
}
