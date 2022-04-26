package requestid

import (
	"context"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/renstrom/shortuuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Request IDs are embedded in HTTP headers using this key.
// This is the standard key used for request Ids. For example, opentelemetry uses the same one.
const MetadataKey = "x-request-id"

// FromContext returns the request Id embedded in gRPC metadata stored in a context,
// if one is available. The second return value is true if the operation was successful.
func FromContext(ctx context.Context) (string, bool) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", false
	}

	ids, ok := md[MetadataKey]
	if !ok || len(ids) == 0 {
		return "", false
	}

	return ids[0], true
}

// FromContextOrMissing returns the request Id embedded in gRPC metadata stored in a context,
// if one is available. If none is available, the string "missing" is returned.
func FromContextOrMissing(ctx context.Context) string {
	if id, ok := FromContext(ctx); ok {
		return id
	}
	return "missing"
}

// AddToIncomingContext returns a new context derived from ctx that is annotated with an Id.
// The Id is stored in the request gRPC metadata. If ctx already has an Id, it is overwritten.
// The second return value is true if the operation was successful.
func AddToIncomingContext(ctx context.Context, id string) (context.Context, bool) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		md.Set(MetadataKey, id)
		return metadata.NewIncomingContext(ctx, md), true
	}
	return ctx, false
}

// UnaryServerInterceptor returns an interceptor that annotates incoming gRPC requests with an Id.
// Ids are stored in the gRPC request metadata and are generated using github.com/renstrom/shortuuid.
// If replace is false, this is only done for requests that do not already have an Id.
func UnaryServerInterceptor(replace bool) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if _, ok := FromContext(ctx); !ok || replace {
			id := shortuuid.New()
			ctx, _ = AddToIncomingContext(ctx, id) // If the operation fails, the original context is returned
		}
		return handler(ctx, req)
	}
}

// StreamServerInterceptor returns an interceptor that annotates incoming gRPC requests with an Id.
// Ids are stored in the gRPC request metadata and are generated using github.com/renstrom/shortuuid.
// If replace is false, this is only done for requests that do not already have an Id.
func StreamServerInterceptor(replace bool) grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if _, ok := FromContext(ctx); !ok || replace {
			id := shortuuid.New()
			ctx, _ = AddToIncomingContext(ctx, id) // If the operation fails, the original context is returned
		}
		wrapped := grpc_middleware.WrapServerStream(stream)
		wrapped.WrappedContext = ctx
		return handler(srv, wrapped)
	}
}
