package armadacontext

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/requestid"
)

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		armadaCtx := createArmadaCtx(ctx)
		return handler(armadaCtx, req)
	}
}

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		armadaCtx := createArmadaCtx(stream.Context())
		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = armadaCtx
		return handler(srv, wrapped)
	}
}

func createArmadaCtx(ctx context.Context) *armadacontext.Context {
	log := logging.NewLogger().
		With("requestId", requestid.FromContextOrMissing(ctx)).
		With("user", auth.GetPrincipal(ctx).GetName())
	return armadacontext.New(ctx, log)
}
