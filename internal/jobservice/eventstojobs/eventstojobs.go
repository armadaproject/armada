package eventstojobs

import (
	"context"
	"io"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/jobservice/repository"
	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	client.WithConnection(&eventToJobService.apiConnection, func(conn *grpc.ClientConn) error {
		eventsClient := api.NewEventClient(conn)
		for {

			clientStream, e := eventsClient.GetJobSetEvents(context,
				&api.JobSetRequest{
					Queue:          eventToJobService.queue,
					Id:             eventToJobService.jobsetid,
					FromMessageId:  "",
					Watch:          false,
					ErrorIfMissing: true,
				},
			)

			if e != nil {
				log.Error(e)
				time.Sleep(5 * time.Second)
				continue
			}

			for {

				msg, e := clientStream.Recv()
				if e != nil {
					if err, ok := status.FromError(e); ok {
						switch err.Code() {
						case codes.NotFound:
							log.Error(err.Message())
							return e
						case codes.PermissionDenied:
							log.Error(err.Message())
							return e
						}
					}
					if e == io.EOF {
						return e
					}
					time.Sleep(5 * time.Second)
					break
				}
				if !IsEventAJobResponse(*msg.Message) {
					continue
				} else {
					jobStatus, eventJobErr := EventsToJobResponse(*msg.Message)
					if eventJobErr != nil {
						// This can mean that the event type reported from server is unknown to the client
						log.Error(eventJobErr)
						continue
					}
					e := eventToJobService.jobServiceRepository.UpdateJobServiceDb(api.JobIdFromApiEvent(msg.Message), jobStatus)
					if e != nil {
						log.Error(e)
						continue
					}
				}

			}
		}
	})
	return nil
}
