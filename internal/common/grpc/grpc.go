package grpc

import (
	"runtime/debug"
	"time"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_logrus "github.com/grpc-ecosystem/go-grpc-middleware/logging/logrus"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/authorization"
)

func CreateGrpcServer(authServices []authorization.AuthService) *grpc.Server {
	unaryInterceptors := []grpc.UnaryServerInterceptor{}
	streamInterceptors := []grpc.StreamServerInterceptor{}

	messageDefault := log.NewEntry(log.StandardLogger())
	tagsExtractor := grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)
	unaryInterceptors = append(unaryInterceptors,
		grpc_ctxtags.UnaryServerInterceptor(tagsExtractor),
		grpc_logrus.UnaryServerInterceptor(messageDefault))
	streamInterceptors = append(streamInterceptors,
		grpc_ctxtags.StreamServerInterceptor(tagsExtractor),
		grpc_logrus.StreamServerInterceptor(messageDefault))

	authFunction := authorization.CreateMiddlewareAuthFunction(authServices)
	unaryInterceptors = append(unaryInterceptors, grpc_auth.UnaryServerInterceptor(authFunction))
	streamInterceptors = append(streamInterceptors, grpc_auth.StreamServerInterceptor(authFunction))

	grpc_prometheus.EnableHandlingTimeHistogram()
	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)

	recovery := grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor(recovery))
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor(recovery))

	return grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 5 * time.Minute,
		}),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)))
}

func panicRecoveryHandler(p interface{}) (err error) {
	log.Errorf("Request triggered panic with cause %v \n%s", p, string(debug.Stack()))
	return status.Errorf(codes.Internal, "Internal server error caused by %v", p)
}
