//go:generate moq -out client_moq.go . JobEventReader
package events

import (
	"context"
	"sync"

	"google.golang.org/grpc"

	"github.com/gogo/protobuf/types"

	"github.com/armadaproject/armada/internal/common/grpc/grpcpool"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
)

// JobEventReader is the interface for retrieving job set event messages
type JobEventReader interface {
	GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error)
	Health(ctx context.Context, empty *types.Empty) (*api.HealthCheckResponse, error)
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
func (ec *EventClient) GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
	err := ec.ensureApiConnection()
	if err != nil {
		return nil, err
	}
	eventClient := api.NewEventClient(ec.conn)

	stream, err := eventClient.GetJobSetEvents(ctx, jobReq)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (ec *EventClient) Health(ctx context.Context, empty *types.Empty) (*api.HealthCheckResponse, error) {
	err := ec.ensureApiConnection()
	if err != nil {
		return nil, err
	}
	eventClient := api.NewEventClient(ec.conn)

	health, err := eventClient.Health(ctx, empty)
	return health, err
}

// Close will close the api connection if established
func (ec *EventClient) Close() {
	ec.mux.Lock()
	defer ec.mux.Unlock()

	if ec.hasConn() {
		ec.conn.Close()
		ec.conn = nil
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

type PooledEventClient struct {
	pool *grpcpool.Pool
}

func NewPooledEventClient(pool *grpcpool.Pool) *PooledEventClient {
	return &PooledEventClient{
		pool: pool,
	}
}

// GetJobEventMessage performs all the steps for obtaining an event message
func (pec *PooledEventClient) GetJobEventMessage(ctx context.Context, jobReq *api.JobSetRequest) (api.Event_GetJobSetEventsClient, error) {
	cc, err := pec.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	eventClient := api.NewEventClient(cc.ClientConn)

	stream, err := eventClient.GetJobSetEvents(ctx, jobReq)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

func (pec *PooledEventClient) Health(ctx context.Context, empty *types.Empty) (*api.HealthCheckResponse, error) {
	cc, err := pec.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	defer cc.Close()
	eventClient := api.NewEventClient(cc.ClientConn)

	health, err := eventClient.Health(ctx, empty)
	if err != nil {
		cc.Unhealthy()
		return nil, err
	}
	return health, err
}

func (ec *PooledEventClient) Close() {}
