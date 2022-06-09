// Utility for watching events.
package eventwatcher

import (
	"context"
	"io"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

type EventWatcher struct {
	Queue                string
	JobSetName           string
	ApiConnectionDetails *client.ApiConnectionDetails
	C                    chan *api.EventMessage
}

func New(queue string, jobSetName string, apiConnectionDetails *client.ApiConnectionDetails) *EventWatcher {
	return &EventWatcher{
		Queue:                queue,
		JobSetName:           jobSetName,
		ApiConnectionDetails: apiConnectionDetails,
		C:                    make(chan *api.EventMessage),
	}
}

// WatchEvents listens for event for the provided queue and job set.
// Received events are forwarded on the channel.
func (srv *EventWatcher) Run(ctx context.Context) error {
	return client.WithEventClient(srv.ApiConnectionDetails, func(c api.EventClient) error {
		stream, err := c.GetJobSetEvents(ctx, &api.JobSetRequest{
			Id:    srv.JobSetName,
			Queue: srv.Queue,
			Watch: true,
		})
		if err != nil {
			return err
		}
		for {
			msg, err := stream.Recv()
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}
			srv.C <- msg.Message
		}
	})
}
