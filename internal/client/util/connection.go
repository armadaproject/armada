package util

import (
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/G-Research/k8s-batch/internal/client/domain"
	"github.com/G-Research/k8s-batch/internal/common/oidc"
)

func WithConnection(apiConnectionDetails *domain.ArmadaApiConnectionDetails, action func(*grpc.ClientConn)) {
	conn, err := createConnection(apiConnectionDetails)

	if err != nil {
		log.Errorf("Failed to connect to api because %s", err)
		return
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

	if connectionDetails.OpenIdConnect.ProviderUrl != "" {
		tokenCredentials, err := oidc.AuthenticatePkce(connectionDetails.OpenIdConnect)
		if err != nil {
			return nil, err
		}
		return grpc.Dial(
			connectionDetails.ArmadaUrl,
			transportCredentials(connectionDetails.ArmadaUrl, true),
			grpc.WithPerRPCCredentials(tokenCredentials),
			unuaryInterceptors,
			streamInterceptors)
	}
	if creds.Username == "" || creds.Password == "" {
		return grpc.Dial(
			connectionDetails.ArmadaUrl,
			transportCredentials(connectionDetails.ArmadaUrl, true),
			unuaryInterceptors,
			streamInterceptors,
		)
	}
	return grpc.Dial(
		connectionDetails.ArmadaUrl,
		transportCredentials(connectionDetails.ArmadaUrl, false),
		grpc.WithPerRPCCredentials(&creds),
		unuaryInterceptors,
		streamInterceptors,
	)
}

func transportCredentials(url string, secure bool) grpc.DialOption {
	if secure && !strings.Contains(url, "localhost") {
		return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return grpc.WithInsecure()
}
