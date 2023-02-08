package grpc

import (
	"context"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
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
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/requestid"
)

// CreateGrpcServer creates a gRPC server (by calling grpc.NewServer) with settings specific to
// this project, and registers services for, e.g., logging and authentication.
func CreateGrpcServer(
	keepaliveParams keepalive.ServerParameters,
	keepaliveEnforcementPolicy keepalive.EnforcementPolicy,
	authServices []authorization.AuthService,
) *grpc.Server {
	// Logging, authentication, etc. are implemented via gRPC interceptors
	// (i.e., via functions that are called before handling the actual request).
	// There are separate interceptors for unary and streaming gRPC calls.
	unaryInterceptors := []grpc.UnaryServerInterceptor{}
	streamInterceptors := []grpc.StreamServerInterceptor{}

	// Automatically recover from panics
	// NOTE This must be the first interceptor, so it can handle panics in any subsequently added interceptor
	recovery := grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)
	unaryInterceptors = append(unaryInterceptors, grpc_recovery.UnaryServerInterceptor(recovery))
	streamInterceptors = append(streamInterceptors, grpc_recovery.StreamServerInterceptor(recovery))

	// Logging (using logrus)
	// By default, information contained in the request context is logged
	// tagsExtractor pulls information out of the request payload (a protobuf) and stores it in
	// the context, such that it is logged.
	messageDefault := log.NewEntry(log.StandardLogger())
	tagsExtractor := grpc_ctxtags.WithFieldExtractor(grpc_ctxtags.CodeGenRequestFieldExtractor)
	unaryInterceptors = append(unaryInterceptors,
		grpc_ctxtags.UnaryServerInterceptor(tagsExtractor),
		requestid.UnaryServerInterceptor(false),
		armadaerrors.UnaryServerInterceptor(2000),
		grpc_logrus.UnaryServerInterceptor(messageDefault),
		logging.UnaryServerInterceptor(),
	)
	streamInterceptors = append(streamInterceptors,
		grpc_ctxtags.StreamServerInterceptor(tagsExtractor),
		requestid.StreamServerInterceptor(false),
		armadaerrors.StreamServerInterceptor(2000),
		grpc_logrus.StreamServerInterceptor(messageDefault),
		logging.StreamServerInterceptor(),
	)

	// Authentication
	// The provided authServices represents a list of services that can be used to authenticate
	// the client (e.g., username/password and OpenId). authFunction is a combination of these.
	authFunction := authorization.CreateMiddlewareAuthFunction(authServices)
	unaryInterceptors = append(unaryInterceptors, grpc_auth.UnaryServerInterceptor(authFunction))
	streamInterceptors = append(streamInterceptors, grpc_auth.StreamServerInterceptor(authFunction))

	// Prometheus timeseries collection integration
	grpc_prometheus.EnableHandlingTimeHistogram()
	unaryInterceptors = append(unaryInterceptors, grpc_prometheus.UnaryServerInterceptor)
	streamInterceptors = append(streamInterceptors, grpc_prometheus.StreamServerInterceptor)

	// Interceptors are registered at server creation
	return grpc.NewServer(
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepaliveEnforcementPolicy),
		grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(streamInterceptors...)),
		grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(unaryInterceptors...)),
	)
}

// TODO We don't need this function. Just do this at the caller.
func Listen(port uint16, grpcServer *grpc.Server, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil { // TODO Don't call fatal, return an error.
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		defer log.Println("Stopping server.")

		log.Printf("Grpc listening on %d", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()
}

// CreateShutdownHandler returns a function that shuts down the grpcServer when the context is closed.
// The server is given gracePeriod to perform a graceful showdown and is then forcably stopped if necessary
func CreateShutdownHandler(ctx context.Context, gracePeriod time.Duration, grpcServer *grpc.Server) func() error {
	return func() error {
		<-ctx.Done()
		go func() {
			time.Sleep(gracePeriod)
			grpcServer.Stop()
		}()
		grpcServer.GracefulStop()
		return nil
	}
}

// This function is called whenever a gRPC handler panics.
func panicRecoveryHandler(p interface{}) (err error) {
	log.Errorf("Request triggered panic with cause %v \n%s", p, string(debug.Stack()))
	return status.Errorf(codes.Internal, "Internal server error caused by %v", p)
}
