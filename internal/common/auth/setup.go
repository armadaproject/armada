package auth

import (
	"context"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

func ConfigureAuth(config configuration.AuthConfig) ([]AuthService, error) {
	var authServices []AuthService

	if len(config.BasicAuth.Users) > 0 {
		authServices = append(authServices,
			NewBasicAuthService(config.BasicAuth.Users))
	}

	if config.KubernetesAuth.KidMappingFileLocation != "" {
		kubernetesAuthService := NewKubernetesNativeAuthService(config.KubernetesAuth)
		authServices = append(authServices, &kubernetesAuthService)
	}

	if config.OpenIdAuth.ProviderUrl != "" {
		openIdAuthService, err := NewOpenIdAuthServiceForProvider(context.Background(), &config.OpenIdAuth)
		if err != nil {
			return nil, errors.WithMessage(err, "error initialising openId auth")
		}
		authServices = append(authServices, openIdAuthService)
	}

	if config.AnonymousAuth {
		authServices = append(authServices, &AnonymousAuthService{})
	}

	if len(authServices) == 0 {
		return nil, errors.New("at least one auth method must be specified in config")
	}

	return authServices, nil
}
