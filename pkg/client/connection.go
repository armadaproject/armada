package client

import (
	"strings"
	"time"

	"github.com/G-Research/armada/pkg/client/auth/exec"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/pkg/client/auth/kerberos"
	"github.com/G-Research/armada/pkg/client/auth/oidc"
)

type ApiConnectionDetails struct {
	ArmadaUrl                   string
	BasicAuth                   common.LoginCredentials
	OpenIdAuth                  oidc.PKCEDetails
	OpenIdDeviceAuth            oidc.DeviceDetails
	OpenIdPasswordAuth          oidc.ClientPasswordDetails
	OpenIdClientCredentialsAuth oidc.ClientCredentialsDetails
	KerberosAuth                kerberos.ClientConfig
	ForceNoTls                  bool
	ExecAuth                    exec.CommandDetails
}

type ConnectionDetails func() *ApiConnectionDetails

func CreateApiConnection(config *ApiConnectionDetails, additionalDialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	return CreateApiConnectionWithCallOptions(config, []grpc.CallOption{}, additionalDialOptions...)
}

func CreateApiConnectionWithCallOptions(
	config *ApiConnectionDetails,
	additionalDefaultCallOptions []grpc.CallOption,
	additionalDialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {

	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(1 * time.Second)),
		grpc_retry.WithMax(3),
	}

	callOptions := append(additionalDefaultCallOptions, grpc.WaitForReady(true))

	defaultCallOptions := grpc.WithDefaultCallOptions(callOptions...)
	unuaryInterceptors := grpc.WithChainUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...))
	streamInterceptors := grpc.WithChainStreamInterceptor(grpc_retry.StreamClientInterceptor(retryOpts...))

	dialOpts := append(additionalDialOptions,
		defaultCallOptions,
		unuaryInterceptors,
		streamInterceptors,
		transportCredentials(config))

	creds, err := perRpcCredentials(config)
	if err != nil {
		return nil, err
	}
	if creds != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(creds))
	}

	return grpc.Dial(config.ArmadaUrl, dialOpts...)
}

func perRpcCredentials(config *ApiConnectionDetails) (credentials.PerRPCCredentials, error) {
	if config.BasicAuth.Username != "" {
		return &config.BasicAuth, nil

	} else if config.OpenIdAuth.ProviderUrl != "" {
		return oidc.AuthenticatePkce(config.OpenIdAuth)

	} else if config.OpenIdDeviceAuth.ProviderUrl != "" {
		return oidc.AuthenticateDevice(config.OpenIdDeviceAuth)

	} else if config.OpenIdPasswordAuth.ProviderUrl != "" {
		return oidc.AuthenticateWithPassword(config.OpenIdPasswordAuth)

	} else if config.OpenIdClientCredentialsAuth.ProviderUrl != "" {
		return oidc.AuthenticateWithClientCredentials(config.OpenIdClientCredentialsAuth)

	} else if config.KerberosAuth.Enabled {
		return kerberos.NewSPNEGOCredentials(config.ArmadaUrl, config.KerberosAuth)
	} else if config.ExecAuth.Cmd != "" {
		return exec.NewAuthenticator(config.ExecAuth), nil
	}
	return nil, nil
}

func transportCredentials(config *ApiConnectionDetails) grpc.DialOption {
	if !config.ForceNoTls && !strings.Contains(config.ArmadaUrl, "localhost") {
		return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return grpc.WithInsecure()
}
