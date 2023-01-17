package logging

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus/ctxlogrus"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	"github.com/armadaproject/armada/internal/common/requestid"
)

// UnaryServerInterceptor returns an interceptor that adds the request id as a
// field to the logrus logger embedded in the context. If an error occurs in the handler,
// it also adds a stack trace.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if id, ok := requestid.FromContext(ctx); ok {
			ctxlogrus.AddFields(ctx, logrus.Fields{requestid.MetadataKey: id})
		}
		rv, err := handler(ctx, req)
		if err != nil {
			// %+v prints a stack trace for pkg/errors errors
			ctxlogrus.AddFields(ctx, logrus.Fields{"errorVerbose": fmt.Sprintf("%+v", err)})
		}
		return rv, err
	}
}

// StreamServerInterceptor returns an interceptor that adds the request id as a
// field to the logrus logger embedded in the context. If an error occurs in the handler,
// it also adds a stack trace.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		ctx := stream.Context()
		if id, ok := requestid.FromContext(ctx); ok {
			ctxlogrus.AddFields(ctx, logrus.Fields{requestid.MetadataKey: id})
		}
		// The logrus logger only logs at the end of the call.  As streaming calls may last a long time
		// We also log here which will produce a log line at the start of the call
		ctxlogrus.Extract(ctx).Infof("started streaming call")
		err := handler(srv, stream)
		if err != nil {
			// %+v prints a stack trace for pkg/errors errors
			ctxlogrus.AddFields(ctx, logrus.Fields{"errorVerbose": fmt.Sprintf("%+v", err)})
		}
		return err
	}
}
