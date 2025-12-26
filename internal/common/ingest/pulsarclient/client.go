// This file is largely a copy of https://github.com/apache/pulsar-client-go/blob/230d11b82ba8b60c971013516c4922afea4a022d/pulsaradmin/pkg/admin/admin.go#L1
//  With simplifications to only cover the functionality we need
// This is to work around a bug in the client which is tracked here:
//  https://github.com/apache/pulsar-client-go/pull/1419
// If pulsar-client-go fix the issue, we should move back to the standard pulsaradmin.Client

package pulsarclient

import (
	"net/http"
	"net/url"
	"path"
	"time"

	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/auth"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/admin/config"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/rest"
	"github.com/apache/pulsar-client-go/pulsaradmin/pkg/utils"
)

const (
	DefaultWebServiceURL       = "http://localhost:8080"
	DefaultHTTPTimeOutDuration = 5 * time.Minute
	ReleaseVersion             = "None"
)

type PulsarClient struct {
	Client     *rest.Client
	APIVersion config.APIVersion
}

type Client interface {
	Subscriptions() Subscriptions
}

// New returns a new client
func New(config *config.Config) (*PulsarClient, error) {
	authProvider, err := auth.GetAuthProvider(config)
	if err != nil {
		return nil, err
	}
	return NewPulsarClientWithAuthProvider(config, authProvider)
}

// NewPulsarClientWithAuthProvider create a client with auth provider.
func NewPulsarClientWithAuthProvider(config *config.Config, authProvider auth.Provider) (*PulsarClient, error) {
	if len(config.WebServiceURL) == 0 {
		config.WebServiceURL = DefaultWebServiceURL
	}

	return &PulsarClient{
		APIVersion: config.PulsarAPIVersion,
		Client: &rest.Client{
			ServiceURL:  config.WebServiceURL,
			VersionInfo: ReleaseVersion,
			HTTPClient: &http.Client{
				Timeout:   DefaultHTTPTimeOutDuration,
				Transport: authProvider,
			},
		},
	}, nil
}

func (c *PulsarClient) endpoint(componentPath string, parts ...string) string {
	escapedParts := make([]string, len(parts))
	for i, part := range parts {
		escapedParts[i] = url.PathEscape(part)
	}
	return path.Join(
		utils.MakeHTTPPath(c.APIVersion.String(), componentPath),
		path.Join(escapedParts...),
	)
}
