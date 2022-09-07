//go:generate moq -out client_moq.go . JobEventReader
package events

import (
	"context"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
	"google.golang.org/grpc"

	log "github.com/sirupsen/logrus"
)

type JobEventReader interface {
	GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (*api.EventStreamMessage, error)
	Close() 
}

type EventClient struct {
	config *client.ApiConnectionDetails
	conn   *grpc.ClientConn
}

func NewEventClient(config *client.ApiConnectionDetails) *EventClient {
	return &EventClient{
		config: config,
	}
}

func (ec *EventClient) GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (*api.EventStreamMessage, error) {
	err := ec.ensureApiConnection()
	if err != nil {
		return nil, err
	}
	eventClient := api.NewEventClient(ec.conn)
	stream, err := eventClient.GetJobSetEvents(ctx, jobReq)
	if err	!= nil {
		return nil, err
	}
	return stream.Recv()
}

func (ec *EventClient) Close() {
	if ec.conn != nil {
		ec.Close()
	}
}

func (ec *EventClient) ensureApiConnection() error {
	if ec.conn != nil {
		return nil
	}
	conn, connErr := client.CreateApiConnection(ec.config)
	if connErr != nil {
		log.Warnf("Connection Issues with EventClient %v", connErr)
		return connErr
	}
	ec.conn = conn
	return nil
}
