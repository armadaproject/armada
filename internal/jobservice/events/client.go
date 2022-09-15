//go:generate moq -out client_moq.go . JobEventReader
package events

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/G-Research/armada/pkg/api"
	"github.com/G-Research/armada/pkg/client"
)

// JobEventReader is the interface for retrieving job set event messages
type JobEventReader interface {
	GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (*api.EventStreamMessage, error)
	Close()
}

// EventClient is the local struct for retrieving events from the api using the grpc client
type EventClient struct {
	config *client.ApiConnectionDetails
	conn   *grpc.ClientConn
	mux    *sync.Mutex
}

// NewEventClient returns a new EventClient
func NewEventClient(config *client.ApiConnectionDetails) *EventClient {
	return &EventClient{
		config: config,
		mux:    &sync.Mutex{},
	}
}

// GetJobEventMessage performs all the steps for obtaining an event message
func (ec *EventClient) GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (*api.EventStreamMessage, error) {
	err := ec.ensureApiConnection()
	if err != nil {
		return nil, err
	}
	eventClient := api.NewEventClient(ec.conn)
	stream, err := eventClient.GetJobSetEvents(ctx, jobReq)
	if err != nil {
		return nil, err
	}
	return stream.Recv()
}

// Close will close the api connection if established
func (ec *EventClient) Close() {
	if ec.hasConn() {
		ec.Close()
	}
}

// hasConn tests whether client already has an api conn
func (ec *EventClient) hasConn() bool {
	return ec.conn != nil
}

// ensureApiConnection will establish api connection if needed
func (ec *EventClient) ensureApiConnection() error {
	if ec.hasConn() {
		return nil
	}

	ec.mux.Lock()
	defer ec.mux.Unlock()

	conn, connErr := client.CreateApiConnection(ec.config)
	if connErr != nil {
		return connErr
	}
	ec.conn = conn
	return nil
}
