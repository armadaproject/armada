package events

import (
	"context"
	"net"
	"testing"

	"github.com/armadaproject/armada/internal/common/grpc/grpcpool"
	"github.com/armadaproject/armada/pkg/api"
	"github.com/armadaproject/armada/pkg/client"
	"github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type DummyEventServer struct{}

func (des *DummyEventServer) GetJobSetEvents(request *api.JobSetRequest, stream api.Event_GetJobSetEventsServer) error {
	return stream.Send(&api.EventStreamMessage{
		Id:      "1",
		Message: &api.EventMessage{},
	})
}

func (des *DummyEventServer) Health(ctx context.Context, cont_ *types.Empty) (*api.HealthCheckResponse, error) {
	return new(api.HealthCheckResponse), nil
}

func (des *DummyEventServer) Report(ctx context.Context, message *api.EventMessage) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (des *DummyEventServer) ReportMultiple(ctx context.Context, message *api.EventList) (*types.Empty, error) {
	return &types.Empty{}, nil
}

func (des *DummyEventServer) Watch(req *api.WatchRequest, stream api.Event_WatchServer) error {
	return nil
}

func startTestGrpcServer(t *testing.T) *grpc.Server {
	grpcServer := grpc.NewServer()
	dummyEventServer := DummyEventServer{}
	api.RegisterEventServer(grpcServer, &dummyEventServer)
	lis, err := net.Listen("tcp", ":31337")
	require.NoError(t, err)
	go func() {
		defer grpcServer.Stop()
		err = grpcServer.Serve(lis)
		require.NoError(t, err)
	}()
	return grpcServer
}

func connFactory() (*grpc.ClientConn, error) {
	return client.CreateApiConnection(&client.ApiConnectionDetails{
		ArmadaUrl:  "localhost:31337",
		ForceNoTls: true,
	})
}

func getTestPool() (*grpcpool.Pool, error) {
	return grpcpool.New(connFactory, 5, 5, 0)
}

func TestPooledEventClient(t *testing.T) {
	_ = startTestGrpcServer(t)

	pool, err := getTestPool()
	require.NoError(t, err)

	client := NewPooledEventClient(pool)

	// This ensures the pooled client will cycle through every connection in the pool.
	for i := 0; i < 10; i++ {
		response, err := client.Health(context.Background(), &types.Empty{})
		require.NoError(t, err)
		require.Equal(t, &api.HealthCheckResponse{}, response)

		eventMessage, err := client.GetJobEventMessage(context.Background(), &api.JobSetRequest{})
		require.NoError(t, err)
		require.Equal(t, "1", eventMessage.Id)
		require.NotNil(t, eventMessage.Message)
	}
}
