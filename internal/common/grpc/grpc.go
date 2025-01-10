package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"runtime/debug"
	"sync"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	grpc_logging "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	grpc_recovery "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/recovery"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadacontext"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth"
	"github.com/armadaproject/armada/internal/common/certs"
	"github.com/armadaproject/armada/internal/common/grpc/configuration"
	log "github.com/armadaproject/armada/internal/common/logging"
	"github.com/armadaproject/armada/internal/common/requestid"
)

// CreateGrpcServer creates a gRPC server (by calling grpc.NewServer) with settings specific to
// this project, and registers services for, e.g., logging and authentication.
func CreateGrpcServer(
	keepaliveParams keepalive.ServerParameters,
	keepaliveEnforcementPolicy keepalive.EnforcementPolicy,
	authServices []auth.AuthService,
	tlsConfig configuration.TlsConfig,
	loggerOpts ...grpc_logging.Option,
) *grpc.Server {
	authFunction := auth.CreateGrpcMiddlewareAuthFunction(auth.NewMultiAuthService(authServices))
	srvMetrics := setupPromMetrics()

	loggerOpts = append(
		loggerOpts,
		grpc_logging.WithLogOnEvents(grpc_logging.StartCall, grpc_logging.FinishCall))

	return grpc.NewServer(
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepaliveEnforcementPolicy),
		setupTls(tlsConfig),
		grpc.ChainUnaryInterceptor(
			srvMetrics.UnaryServerInterceptor(),
			requestid.UnaryServerInterceptor(false),
			grpc_auth.UnaryServerInterceptor(authFunction),
			grpc_logging.UnaryServerInterceptor(InterceptorLogger(), loggerOpts...),
			armadaerrors.UnaryServerInterceptor(2000),
			grpc_recovery.UnaryServerInterceptor(grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
		grpc.ChainStreamInterceptor(
			srvMetrics.StreamServerInterceptor(),
			requestid.StreamServerInterceptor(false),
			grpc_auth.StreamServerInterceptor(authFunction),
			grpc_logging.StreamServerInterceptor(InterceptorLogger(), loggerOpts...),
			armadaerrors.StreamServerInterceptor(2000),
			grpc_recovery.StreamServerInterceptor(grpc_recovery.WithRecoveryHandler(panicRecoveryHandler)),
		),
	)
}

func setupPromMetrics() *grpc_prometheus.ServerMetrics {
	srvMetrics := grpc_prometheus.NewServerMetrics(
		grpc_prometheus.WithServerHandlingTimeHistogram(
			grpc_prometheus.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120}),
		),
	)
	reg := prometheus.NewRegistry()
	reg.MustRegister(srvMetrics)
	return srvMetrics
}

// TODO We don't need this function. Just do this at the caller.
func Listen(port uint16, grpcServer *grpc.Server, wg *sync.WaitGroup) {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil { // TODO Don't call fatal, return an error.
		log.Fatalf("failed to listen: %v", err)
	}

	go func() {
		defer log.Infof("Stopping server.")

		log.Infof("Grpc listening on %d", port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}

		wg.Done()
	}()
}

// CreateShutdownHandler returns a function that shuts down the grpcServer when the context is closed.
// The server is given gracePeriod to perform a graceful showdown and is then forcably stopped if necessary
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

func InterceptorLogger() grpc_logging.Logger {
	return grpc_logging.LoggerFunc(func(ctx context.Context, lvl grpc_logging.Level, msg string, fields ...any) {
		logFields := make(map[string]any, len(fields)/2+2)
		logFields["user"] = ctx.Value("user")
		logFields["requestId"] = ctx.Value("requestId")
		i := grpc_logging.Fields(fields).Iterator()
		for i.Next() {
			k, v := i.At()
			logFields[k] = v
		}
		l := log.WithFields(logFields)
		switch lvl {
		case grpc_logging.LevelDebug:
			l.Debug(msg)
		case grpc_logging.LevelInfo:
			l.Info(msg)
		case grpc_logging.LevelWarn:
			l.Warn(msg)
		case grpc_logging.LevelError:
			l.Error(msg)
		default:
			panic(fmt.Sprintf("unknown level %v", lvl))
		}
	})
}
