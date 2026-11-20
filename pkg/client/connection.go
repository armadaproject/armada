package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/retry"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/keepalive"

	"github.com/armadaproject/armada/pkg/client/auth/basic"
	"github.com/armadaproject/armada/pkg/client/auth/exec"
	"github.com/armadaproject/armada/pkg/client/auth/kubernetes"
	"github.com/armadaproject/armada/pkg/client/auth/oidc"
)

type ApiConnectionDetails struct {
	ArmadaUrl string
	// LookoutUrl is the url of the Lookout API, for the commands that query Lookout rather than Armada.
	LookoutUrl    string
	ArmadaRestUrl string
	// Names of executor clusters as they appear in the local kubeconfig file.
	// Used by the test suite to download logs from pods running tests.
	ExecutorClusters []string
	// After a duration of this time, if the client doesn't see any activity it
	// pings the server to see if the transport is still alive.
	// If set below 10s, a minimum value of 10s is used instead.
	// The default value is infinity.
	GrpcKeepAliveTime time.Duration
	// After having pinged for keepalive check, the client waits for a duration
	// of Timeout and if no activity is seen even after that the connection is
	// closed.
	GrpcKeepAliveTimeout time.Duration
	// Authentication options.
	BasicAuth                   basic.LoginCredentials
	KubernetesNativeAuth        kubernetes.NativeAuthDetails
	OpenIdAuth                  oidc.PKCEDetails
	OpenIdDeviceAuth            oidc.DeviceDetails
	OpenIdPasswordAuth          oidc.ClientPasswordDetails
	OpenIdClientCredentialsAuth oidc.ClientCredentialsDetails
	OpenIdKubernetesAuth        oidc.KubernetesDetails
	ForceNoTls                  bool
	ExecAuth                    exec.CommandDetails
	CacheRefreshToken           bool
}

type ConnectionDetails func() (*ApiConnectionDetails, error)

func CreateApiConnection(config *ApiConnectionDetails, additionalDialOptions ...grpc.DialOption) (*grpc.ClientConn, error) {
	return CreateApiConnectionWithCallOptions(config, []grpc.CallOption{}, additionalDialOptions...)
}

func CreateApiConnectionWithCallOptions(
	config *ApiConnectionDetails,
	additionalDefaultCallOptions []grpc.CallOption,
	additionalDialOptions ...grpc.DialOption,
) (*grpc.ClientConn, error) {
	retryOpts := []grpc_retry.CallOption{
		grpc_retry.WithBackoff(grpc_retry.BackoffExponential(1 * time.Second)),
		grpc_retry.WithMax(5),
	}

	callOptions := append(additionalDefaultCallOptions, grpc.WaitForReady(true), grpc.UseCompressor(gzip.Name))
	defaultCallOptions := grpc.WithDefaultCallOptions(callOptions...)
	unuaryInterceptors := grpc.WithChainUnaryInterceptor(grpc_retry.UnaryClientInterceptor(retryOpts...))
	streamInterceptors := grpc.WithChainStreamInterceptor(grpc_retry.StreamClientInterceptor(retryOpts...))
	dialOpts := append(additionalDialOptions,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		defaultCallOptions,
		unuaryInterceptors,
		streamInterceptors,
		transportCredentials(config),
	)
	// gRPC keepalive options.
	if config.GrpcKeepAliveTime > 0 || config.GrpcKeepAliveTimeout > 0 {
		keepAliveOptions := grpc.WithKeepaliveParams(
			keepalive.ClientParameters{
				Time:    config.GrpcKeepAliveTime,
				Timeout: config.GrpcKeepAliveTimeout,
			},
		)
		dialOpts = append(dialOpts, keepAliveOptions)
	}

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
	} else if config.KubernetesNativeAuth.Enabled {
		return kubernetes.AuthenticateKubernetesNative(config.KubernetesNativeAuth)
	} else if config.OpenIdAuth.ProviderUrl != "" {
		return oidc.AuthenticatePkce(config.OpenIdAuth, config.CacheRefreshToken)
	} else if config.OpenIdDeviceAuth.ProviderUrl != "" {
		return oidc.AuthenticateDevice(config.OpenIdDeviceAuth, config.CacheRefreshToken)
	} else if config.OpenIdPasswordAuth.ProviderUrl != "" {
		return oidc.AuthenticateWithPassword(config.OpenIdPasswordAuth, config.CacheRefreshToken)
	} else if config.OpenIdClientCredentialsAuth.ProviderUrl != "" {
		return oidc.AuthenticateWithClientCredentials(config.OpenIdClientCredentialsAuth)
	} else if config.OpenIdKubernetesAuth.ProviderUrl != "" {
		return oidc.AuthenticateKubernetes(config.OpenIdKubernetesAuth)
	} else if config.ExecAuth.Cmd != "" {
		return exec.NewAuthenticator(config.ExecAuth), nil
	}
	return nil, nil
}

// RequestAuth logs in with the credentials the connection is configured with, which are the credentials that
// authenticate its gRPC calls, and returns a function that gives the HTTP headers that authenticate a request to a url.
// The login, which may be interactive, happens here, once. The function only asks the credentials for their current
// token, which they renew themselves when it has expired, so it can be called for every request of a long command.
// It gives no headers when no credentials are configured. Like the gRPC connection, which only goes without TLS to a
// local server or when ForceNoTls is set, it refuses to give credentials for a url they must not be sent to over plain
// http.
func (config *ApiConnectionDetails) RequestAuth() (func(ctx context.Context, url string) (map[string]string, error), error) {
	creds, err := perRpcCredentials(config)
	if err != nil {
		return nil, err
	}
	if creds == nil {
		return func(context.Context, string) (map[string]string, error) { return nil, nil }, nil
	}
	return func(ctx context.Context, url string) (map[string]string, error) {
		if !config.AllowsCredentialsOver(url) {
			return nil, errors.New("refusing to send credentials over plain http: use an https url")
		}
		return creds.GetRequestMetadata(ctx, url)
	}, nil
}

// AuthHeaders returns the HTTP headers that authenticate a request to url, as RequestAuth does, for a caller that makes
// one request. A caller that makes several should use RequestAuth, which logs in once.
func (config *ApiConnectionDetails) AuthHeaders(ctx context.Context, url string) (map[string]string, error) {
	authenticate, err := config.RequestAuth()
	if err != nil {
		return nil, err
	}
	return authenticate(ctx, url)
}

// AllowsCredentialsOver reports whether credentials may be sent to rawURL. Like the gRPC connection, which only
// goes without TLS to a local server or when ForceNoTls is set, they may not go over plain http to anything else.
// A url that cannot be parsed is not trusted.
func (config *ApiConnectionDetails) AllowsCredentialsOver(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil {
		return false
	}
	return u.Scheme != "http" || IsLocalAddress(rawURL) || config.ForceNoTls
}

// IsLocalAddress reports whether the host of an address, which may be a url or a host with a port, is this machine.
func IsLocalAddress(address string) bool {
	u, err := url.Parse(address)
	if err != nil || u.Host == "" {
		u, err = url.Parse("//" + address)
		if err != nil {
			return false
		}
	}
	host := u.Hostname()
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func transportCredentials(config *ApiConnectionDetails) grpc.DialOption {
	if !config.ForceNoTls && !strings.Contains(config.ArmadaUrl, "localhost") {
		return grpc.WithTransportCredentials(credentials.NewClientTLSFromCert(nil, ""))
	}
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// ArmadaHealthCheck calls Armada Server /health endpoint.
//
// Returns true if response status code is in range [200-399], otherwise returns false.
//
// TODO: Shouldn't be a method on ApiConnectionDetails.
// TODO: Break this up into one function to get the status code and another to check if that's a healthy one.
func (a *ApiConnectionDetails) ArmadaHealthCheck() (ok bool, err error) {
	url := a.ArmadaRestUrl
	if url == "" {
		return false, errors.New("Armada server rest api url not provided")
	}
	if !strings.HasPrefix(url, "http") {
		url = fmt.Sprintf("http://%s", url)
	}
	healthEndpoint := fmt.Sprintf("%s/health", url)
	resp, err := http.Get(healthEndpoint)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return false, nil
	}

	return true, nil
}
