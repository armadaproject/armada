package eventstojobs

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
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
				return true
			}()

			// Case 2 for exiting: All jobs are finished in job-set
			return state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs()
		})
		return nil
	})
}
func (eventToJobService *EventsToJobService) GetStatusWithoutRedis(context context.Context, jobId string) (*jobservice.JobServiceResponse, error) {
	jobStatusForId := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
	client.WithEventClient(&eventToJobService.jobServiceConfig.ApiConnection, func(c api.EventClient) error {
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
				if api.JobIdFromApiEvent(eventMessage) == jobId {
					jobStatusForId = jobStatus
					log.Infof("Job Id Found Without Redis: %s with State: %s ", jobId, jobStatus.State)
				} else {
					log.Infof("Job Id not found: %s", jobId)
				}
			}
			// Case 1: End if we hit our configurable timeout for subscription.
			go func() bool {
				<-t1.C
				return true
			}()

			// Case 2 for exiting: All jobs are finished in job-set
			return state.GetNumberOfJobs() == state.GetNumberOfFinishedJobs()
		})
		return nil
	})
	return jobStatusForId, nil
}
func (eventToJobService *EventsToJobService) StreamCommon(c api.EventClient, ctx context.Context) error {
	var fromMessageId string

	err := client.WithEventClient(&eventToJobService.jobServiceConfig.ApiConnection, func(c api.EventClient) error {
		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:            eventToJobService.jobsetid,
			Queue:         eventToJobService.queue,
			Watch:         true,
			FromMessageId: fromMessageId,
		})
		if err != nil {
			return err
		}
		for {
			msg, err := stream.Recv()
			if err != nil {
				return err
			}
			fromMessageId = msg.GetId()
			select {
			case <-ctx.Done():
				return ctx.Err()
			}
			
		}
	})
	return err
}
