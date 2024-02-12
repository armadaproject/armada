package auth

import (
	"context"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common/auth/authorization"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

func ConfigureAuth(config configuration.AuthConfig) ([]authorization.AuthService, error) {
	var authServices []authorization.AuthService

	if len(config.BasicAuth.Users) > 0 {
		authServices = append(authServices,
			authorization.NewBasicAuthService(config.BasicAuth.Users))
	}

	if config.KubernetesAuth.KidMappingFileLocation != "" {
		kubernetesAuthService := authorization.NewKubernetesNativeAuthService(config.KubernetesAuth)
		authServices = append(authServices, &kubernetesAuthService)
	}

	if config.OpenIdAuth.ProviderUrl != "" {
		openIdAuthService, err := authorization.NewOpenIdAuthServiceForProvider(context.Background(), &config.OpenIdAuth)
		if err != nil {
			return nil, errors.WithMessage(err, "error initialising openId auth")
		}
		authServices = append(authServices, openIdAuthService)
	}

	if config.AnonymousAuth {
		authServices = append(authServices, &authorization.AnonymousAuthService{})
	}

	if len(authServices) == 0 {
		return nil, errors.New("at least one auth method must be specified in config")
	}

	return authServices, nil
}
