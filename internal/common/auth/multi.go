package auth

import (
	"context"
	"errors"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

type MultiAuthService struct {
	authServices []AuthService
}

func NewMultiAuthService(authServices []AuthService) *MultiAuthService {
	return &MultiAuthService{authServices: authServices}
}

// Authenticate - The services in authServices are tried one at a time in sequence.
// Successful authentication short-circuits the process.
func (multi *MultiAuthService) Authenticate(ctx context.Context, authHeader string) (Principal, error) {
	for _, service := range multi.authServices {
		principal, err := service.Authenticate(ctx, authHeader)

		var missingCredsErr *armadaerrors.ErrMissingCredentials
		if errors.As(err, &missingCredsErr) {
			// try next auth service
			continue
		} else if err != nil {
			return nil, err
		}
		return principal, nil
	}
	return nil, &armadaerrors.ErrUnauthenticated{
		Message: "Request could not be authenticated with any of the supported schemes.",
	}
}
