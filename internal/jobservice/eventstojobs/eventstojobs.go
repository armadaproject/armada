package eventstojobs

import (
	"context"
	"io"
	"time"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/api/jobservice"
	"github.com/G-Research/armada/pkg/client"
	log "github.com/sirupsen/logrus"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EventsToJobService struct {
	queue         string
	jobsetid      string
	jobid         string
	apiConnection client.ApiConnectionDetails
}

func NewEventsToJobService(
	queue string,
	jobsetid string,
	jobid string,
	apiConnection client.ApiConnectionDetails) *EventsToJobService {
	return &EventsToJobService{
		queue:         queue,
		jobsetid:      jobsetid,
		jobid:         jobid,
		apiConnection: apiConnection}
}
// TODO: This function will use redis and a pub/sub method.
// For now, we will use the api for streaming events and allow for clients to treat this as polling.
// This is not a production usecase yet.
func (eventToJobService *EventsToJobService) GetJobStatusUsingEventApi(context context.Context) (*jobservice.JobServiceResponse, error) {
	returnJobService := jobservice.JobServiceResponse{State: "Success"}

	client.WithConnection(&eventToJobService.apiConnection, func(conn *grpc.ClientConn) {
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
							return
						case codes.PermissionDenied:
							log.Error(err.Message())
							return
						}
					}
					if e == io.EOF {
						return
					}
					time.Sleep(5 * time.Second)
					break
				}
				jobStatus, e := EventsToJobResponse(*msg.Message)
				if e != nil {
					// This can mean that the event type reported from server is unknown to the client
					log.Error(e)
					continue
				}
				returnJobService.Error = jobStatus.Error
				returnJobService.State = jobStatus.State
			}
		}
	})
	return &returnJobService, nil
}
