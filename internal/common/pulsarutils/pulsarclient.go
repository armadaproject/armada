package pulsarutils

import (
	"strings"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/apache/pulsar-client-go/pulsaradmin"
	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	commonconfig "github.com/armadaproject/armada/internal/common/config"
	"github.com/armadaproject/armada/internal/common/logging"
)

func NewPulsarAdminClient(config *commonconfig.PulsarConfig) (pulsaradmin.Client, error) {
	tokenPath := ""

	if config.AuthenticationEnabled {
		jwtPath, err := getTokenPath(config)
		if err != nil {
			return nil, err
		}
		tokenPath = jwtPath
	}

	return pulsaradmin.NewClient(&pulsaradmin.Config{
		WebServiceURL:                 config.RestURL,
		TLSTrustCertsFilePath:         config.TLSTrustCertsFilePath,
		TLSEnableHostnameVerification: config.TLSValidateHostname,
		TLSAllowInsecureConnection:    config.TLSAllowInsecureConnection,
		TokenFile:                     tokenPath,
	})
}

func NewPulsarClient(config *commonconfig.PulsarConfig) (pulsar.Client, error) {
	var authentication pulsar.Authentication

	if config.AuthenticationEnabled {
		jwtPath, err := getTokenPath(config)
		if err != nil {
			return nil, err
		}
		authentication = pulsar.NewAuthenticationTokenFromFile(jwtPath)
	}

	return pulsar.NewClient(pulsar.ClientOptions{
		URL:                        config.URL,
		TLSTrustCertsFilePath:      config.TLSTrustCertsFilePath,
		TLSValidateHostname:        config.TLSValidateHostname,
		TLSAllowInsecureConnection: config.TLSAllowInsecureConnection,
		MaxConnectionsPerBroker:    config.MaxConnectionsPerBroker,
		Authentication:             authentication,
		Logger:                     logging.NewPulsarLogger(),
	})
}

func getTokenPath(config *commonconfig.PulsarConfig) (string, error) {
	if strings.ToLower(config.AuthenticationType) != "jwt" {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "pulsar.AuthenticationType",
			Value:   config.AuthenticationType,
			Message: "Only JWT Authentication for Pulsar is supported right now.",
		})
	}
	if strings.TrimSpace(config.JwtTokenPath) == "" {
		return "", errors.WithStack(&armadaerrors.ErrInvalidArgument{
			Name:    "pulsar.JwtTokenPath",
			Value:   config.JwtTokenPath,
			Message: "JWT authentication was configured for Pulsar but no JwtTokenPath was supplied",
		})
	}
	return config.JwtTokenPath, nil
}
