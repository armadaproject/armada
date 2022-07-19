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
		jobIdMap, err := eventToJobService.StreamCommon(c, context)
		for key, element := range jobIdMap {
			log.Infof("key %s element: %s", key, element.State)
			e := eventToJobService.jobServiceRepository.UpdateJobServiceDb(key, element)
			if e != nil {
				panic(e)
			}
		}
		return err
	})
}
func (eventToJobService *EventsToJobService) GetStatusWithoutRedis(context context.Context, jobId string) (*jobservice.JobServiceResponse, error) {
	jobStatusForId := &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
	err := client.WithEventClient(&eventToJobService.jobServiceConfig.ApiConnection, func(c api.EventClient) error {
		jobIdMap, err := eventToJobService.StreamCommon(c, context)
		var ok bool
		jobStatusForId, ok = jobIdMap[jobId]
		if !ok {
			jobStatusForId = &jobservice.JobServiceResponse{State: jobservice.JobServiceResponse_JOB_ID_NOT_FOUND}
		}
		return err
	})
	return jobStatusForId, err
}
func (eventToJobService *EventsToJobService) StreamCommon(c api.EventClient, ctx context.Context) (map[string]*jobservice.JobServiceResponse, error) {
	jobIdMap := make(map[string]*jobservice.JobServiceResponse)
	var fromMessageId string
	// I found that GRPC will not allow you to run something in background and return a value back to caller.
	// GRPC will cancel the context once your request returns.
	// So we are going to introduce a timer for how long to subscribe to event.
	// This will allow the rpc call to listen for all events in a given job-set
	// But we will return after SubscribeJobSetTime s
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(eventToJobService.jobServiceConfig.SubscribeJobSetTime)*time.Second)
	defer cancel()
	err := client.WithEventClient(&eventToJobService.jobServiceConfig.ApiConnection, func(c api.EventClient) error {
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
			// This allows us to subscribe for x amount of time
			case <-ctxTimeout.Done():
				log.Info("Hit a timeout")
				return nil
			default:
			}
			if !IsEventAJobResponse(*msg.Message) {
			} else {
				jobStatus, eventJobErr := EventsToJobResponse(*msg.Message)
				if eventJobErr != nil {
					// This can mean that the event type reported from server is unknown to the client
					log.Error(eventJobErr)
				}
				terminalEventClientId := false
				if eventToJobService.jobid == currentJobId {
					terminalEventClientId = IsEventTerminal(*msg.Message)
				}
				val, ok := jobIdMap[currentJobId]
				if ok && val.State != jobStatus.State {
					jobIdMap[currentJobId] = jobStatus
				} else {
					jobIdMap[currentJobId] = jobStatus
				}
				// If our jobId is finished, we should return.
				if terminalEventClientId {
					return nil
				}
			}
		}
	})
	return jobIdMap, err
}
