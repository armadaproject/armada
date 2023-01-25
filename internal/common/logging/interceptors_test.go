package logging

import (
	"context"
	"testing"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/renstrom/shortuuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/common/requestid"
)

func TestUnaryServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	id := shortuuid.New()
	ctx, ok := requestid.AddToIncomingContext(ctx, id)
	require.True(t, ok, "error adding request id to context")
	logger := logrus.New()
	entry := logrus.NewEntry(logger)
	ctx = ctxlogrus.ToContext(ctx, entry)
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		entry := ctxlogrus.Extract(ctx)
		for _, field := range entry.Data {
			if s, ok := field.(string); ok && s == id {
				return nil, nil
			}
		}
		t.Fatal("request id was not added as a logger field")
		return nil, nil
	}

	f := UnaryServerInterceptor()
	_, err := f(ctx, nil, nil, handler)
	require.NoError(t, err)
}

func TestStreamServerInterceptor(t *testing.T) {
	ctx := context.Background()
	ctx = metadata.NewIncomingContext(ctx, metadata.New(map[string]string{}))
	id := shortuuid.New()
	ctx, ok := requestid.AddToIncomingContext(ctx, id)
	require.True(t, ok, "error adding request id to context")

	logger := logrus.New()
	entry := logrus.NewEntry(logger)
	ctx = ctxlogrus.ToContext(ctx, entry)
	stream := &grpc_middleware.WrappedServerStream{}
	stream.WrappedContext = ctx
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		ctx := stream.Context()
		entry := ctxlogrus.Extract(ctx)
		for _, field := range entry.Data {
			if s, ok := field.(string); ok && s == id {
				return nil
			}
		}
		t.Fatal("request id was not added as a logger field")
		return nil
	}

	f := StreamServerInterceptor()
	err := f(nil, stream, nil, handler)
	require.NoError(t, err)
}
