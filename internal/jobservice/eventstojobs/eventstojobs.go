package eventstojobs

import (
	"context"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/G-Research/armada/internal/jobservice/configuration"
	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// Service that subscribes to events and stores JobStatus in the repository.
type EventsToJobService struct {
	queue                string
	jobSetId             string
	jobId                string
	jobServiceConfig     *configuration.JobServiceConfiguration
	jobServiceRepository repository.SQLJobService
}

func NewEventsToJobService(
	queue string,
	jobSetId string,
	jobId string,
	jobServiceConfig *configuration.JobServiceConfiguration,
	jobServiceRepository repository.SQLJobService,
) *EventsToJobService {
	return &EventsToJobService{
		queue:                queue,
		jobSetId:             jobSetId,
		jobId:                jobId,
		jobServiceConfig:     jobServiceConfig,
		jobServiceRepository: jobServiceRepository,
	}
}

// Subscribes to a JobSet from jobsetid
func (eventToJobService *EventsToJobService) SubscribeToJobSetId(context context.Context) error {
	return eventToJobService.streamCommon(&eventToJobService.jobServiceConfig.ApiConnection, context)
}

func (eventToJobService *EventsToJobService) streamCommon(clientConnect *client.ApiConnectionDetails, ctx context.Context) error {
	var fromMessageId string
	conn, connErr := client.CreateApiConnection(clientConnect)
	if connErr != nil {
		log.Warnf("Connection Issues with EventClient %v", connErr)
		return connErr
	}
	defer conn.Close()
	eventToJobService.jobServiceRepository.SubscribeJobSet(eventToJobService.queue, eventToJobService.jobSetId)
	g, _ := errgroup.WithContext(ctx)
	g.Go(func() error {
		// Once we unsubscribed from the job-set, we need to close the GRPC connection.
		// According to GRPC official docs, you can only end a client stream by either canceling the context or closing the connection
		// This will log an error to the jobservice log saying that the connection was used.
		ticker := time.NewTicker(time.Duration(eventToJobService.jobServiceConfig.SubscribeJobSetTime) * time.Second)
		for range ticker.C {
			if !eventToJobService.jobServiceRepository.IsJobSetSubscribed(eventToJobService.queue, eventToJobService.jobSetId) {
				return conn.Close()
			}
		}
		return nil
	})
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		eventClient := api.NewEventClient(conn)
		stream, err := eventClient.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:             eventToJobService.jobSetId,
			Queue:          eventToJobService.queue,
			Watch:          true,
			FromMessageId:  fromMessageId,
			ErrorIfMissing: false,
		})
		if err != nil {
			log.Errorf("Error found from client %v", err)
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
			jobStatus := EventsToJobResponse(*msg.Message)
			if jobStatus != nil {
				jobTable := repository.NewJobTable(eventToJobService.queue, eventToJobService.jobSetId, currentJobId, *jobStatus)
				eventToJobService.jobServiceRepository.UpdateJobServiceDb(jobTable)
			}
		}
	})
	g.Wait()
	return nil
}
