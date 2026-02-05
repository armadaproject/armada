package grpc

import (
	"context"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/auth"
)

var grpcRequestDurationMs = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "armada_grpc_request_duration_ms",
		Help:    "gRPC request duration in milliseconds split by user.",
		Buckets: []float64{1, 10, 100, 1000, 10000, 100000, 1000000},
	},
	[]string{"user", "grpc_service", "grpc_method", "grpc_code", "grpc_type"},
)

var grpcRequestsTotal = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "armada_grpc_requests_total",
		Help: "Total number of gRPC requests split by user.",
	},
	[]string{"user", "grpc_service", "grpc_method", "grpc_code", "grpc_type"},
)

func userMetricsUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		recordGrpcRequestDuration(ctx, info.FullMethod, "unary", err, time.Since(start))
		return resp, err
	}
}

func userMetricsStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, stream)
		recordGrpcRequestDuration(stream.Context(), info.FullMethod, "stream", err, time.Since(start))
		return err
	}
}

func recordGrpcRequestDuration(ctx context.Context, fullMethod, grpcType string, err error, duration time.Duration) {
	principal := auth.GetPrincipal(ctx)
	user := principal.GetName()
	service, method := splitFullMethod(fullMethod)
	code := status.Code(err).String()
	grpcRequestsTotal.WithLabelValues(user, service, method, code, grpcType).Inc()
	grpcRequestDurationMs.WithLabelValues(user, service, method, code, grpcType).Observe(float64(duration.Microseconds()) / 1000.0)
}

func splitFullMethod(fullMethod string) (service, method string) {
	// fullMethod typically looks like "/package.Service/Method".
	trimmed := strings.TrimPrefix(fullMethod, "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return "unknown", "unknown"
	}
	service = parts[0]
	method = parts[1]
	if service == "" {
		service = "unknown"
	}
	if method == "" {
		method = "unknown"
	}
	return service, method
}
