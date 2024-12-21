package grpc

import (
	"crypto/tls"
	"fmt"
	"github.com/armadaproject/armada/internal/common/certs"
	"google.golang.org/grpc/credentials"
	"net"
	"runtime/debug"
	"sync"
	"time"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/grpc/configuration"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/requestid"
)

// CreateGrpcServer creates a gRPC server with settings specific to this project, and registers services for, e.g.
// logging and authentication.
func CreateGrpcServer(
	keepaliveParams keepalive.ServerParameters,
	keepaliveEnforcementPolicy keepalive.EnforcementPolicy,
	authServices []auth.AuthService,
	tlsConfig configuration.TlsConfig,
) *grpc.Server {

	authFunction := auth.CreateGrpcMiddlewareAuthFunction(auth.NewMultiAuthService(authServices))

	return grpc.NewServer(
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepaliveEnforcementPolicy),
		setupTls(tlsConfig),
		grpc.ChainUnaryInterceptor(
			requestid.UnaryServerInterceptor(false),
			armadaerrors.UnaryServerInterceptor(2000),
			grpc_auth.UnaryServerInterceptor(authFunction),
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
		grpc.ChainStreamInterceptor(
			requestid.StreamServerInterceptor(false),
			armadaerrors.StreamServerInterceptor(2000),
			grpc_auth.StreamServerInterceptor(authFunction),
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
	)

}

func Listen(port uint16, grpcServer *grpc.Server, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil { // TODO Don't call fatal, return an error.
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		defer log.Infof("Stopping server.")

		log.Infof("Grpc listening on %d", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Errorf("failed to serve: %v", err)
		}

		wg.Done()
	}()
}

// CreateShutdownHandler returns a function that shuts down the grpcServer when the context is closed.
// The server is given gracePeriod to perform a graceful showdown and is then forcibly stopped if necessary
func CreateShutdownHandler(ctx *armadacontext.Context, gracePeriod time.Duration, grpcServer *grpc.Server) func() error {
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

func setupTls(tlsConfig configuration.TlsConfig) grpc.ServerOption {
	if !tlsConfig.Enabled {
		return grpc.EmptyServerOption{}
	}

	cachedCertificateService := certs.NewCachedCertificateService(tlsConfig.CertPath, tlsConfig.KeyPath, time.Minute)
	go func() {
		cachedCertificateService.Run(armadacontext.Background())
	}()
	tlsCreds := credentials.NewTLS(&tls.Config{
		GetCertificate: func(info *tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := cachedCertificateService.GetCertificate()
			if cert == nil {
				return nil, fmt.Errorf("unexpectedly received nil from certificate cache")
			}
			return cert, nil
		},
	})
	return grpc.Creds(tlsCreds)
}

// This function is called whenever a gRPC handler panics.
func panicRecoveryHandler(p interface{}) (err error) {
	log.Errorf("Request triggered panic with cause %v \n%s", p, string(debug.Stack()))
	return status.Errorf(codes.Internal, "Internal server error caused by %v", p)
}
