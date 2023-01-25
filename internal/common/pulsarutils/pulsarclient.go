package pulsarutils

import (
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/armada/configuration"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func NewPulsarClient(config *configuration.PulsarConfig) (pulsar.Client, error) {
	var authentication pulsar.Authentication

	// Sanity check that supplied Pulsar authentication parameters make sense
	if config.AuthenticationEnabled {
		if strings.ToLower(config.AuthenticationType) != "jwt" {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "pulsar.AuthenticationType",
				Value:   config.AuthenticationType,
				Message: "Only JWT Authentication for Pulsar is supported right now.",
			})
		}
		if strings.TrimSpace(config.JwtTokenPath) == "" {
			return nil, errors.WithStack(&armadaerrors.ErrInvalidArgument{
				Name:    "pulsar.JwtTokenPath",
				Value:   config.JwtTokenPath,
				Message: "JWT authentication was configured for Pulsar but no JwtTokenPath was supplied",
			})
		}
		authentication = pulsar.NewAuthenticationTokenFromFile(config.JwtTokenPath)
	}

	return pulsar.NewClient(pulsar.ClientOptions{
		URL:                        config.URL,
		TLSTrustCertsFilePath:      config.TLSTrustCertsFilePath,
		TLSValidateHostname:        config.TLSValidateHostname,
		TLSAllowInsecureConnection: config.TLSAllowInsecureConnection,
		MaxConnectionsPerBroker:    config.MaxConnectionsPerBroker,
		Authentication:             authentication,
	})
}
