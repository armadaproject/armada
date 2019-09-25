package util

import (
	"github.com/G-Research/k8s-batch/internal/client/domain"
	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"time"
)

func WithConnection(apiConnectionDetails *domain.ArmadaApiConnectionDetails, action func(*grpc.ClientConn)) {
	conn, err := createConnection(apiConnectionDetails)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
	}
	defer conn.Close()

	action(conn)
}

func createConnection(connectionDetails *domain.ArmadaApiConnectionDetails) (*grpc.ClientConn, error) {
	creds := connectionDetails.Credentials

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(300 * time.Millisecond)),
		grpc_retry.WithMax(3),
	}

	unuaryInterceptors := grpc.WithChainUnaryInterceptor(
		grpc_retry.UnaryClientInterceptor(retryOpts...),
	)

	streamInterceptors := grpc.WithChainStreamInterceptor(
		grpc_retry.StreamClientInterceptor(retryOpts...),
	)

	if creds.Username == "" || creds.Password == "" {
		return grpc.Dial(
			connectionDetails.Url,
			grpc.WithInsecure(),
			unuaryInterceptors,
			streamInterceptors,
		)
	} else {
		return grpc.Dial(
			connectionDetails.Url,
			grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, "")),
			grpc.WithPerRPCCredentials(&creds),
			unuaryInterceptors,
			streamInterceptors,
		)
	}
}
