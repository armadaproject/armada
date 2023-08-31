package authorization

import (
	"encoding/base64"
	"github.com/armadaproject/armada/internal/common/context"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

type BasicAuthService struct {
	users map[string]configuration.UserInfo
}

func NewBasicAuthService(users map[string]configuration.UserInfo) *BasicAuthService {
	return &BasicAuthService{users: users}
}

func (authService *BasicAuthService) Name() string {
	return "Basic"
}

func (authService *BasicAuthService) Authenticate(ctx *context.ArmadaContext) (Principal, error) {
	basicAuth, err := grpc_auth.AuthFromMD(ctx, "basic")
	if err == nil {
		payload, err := base64.StdEncoding.DecodeString(basicAuth)
		if err != nil {
			return nil, &armadaerrors.ErrInvalidCredentials{
				AuthService: authService.Name(),
				Message:     err.Error(),
			}
		}
		pair := strings.SplitN(string(payload), ":", 2)
		return authService.loginUser(pair[0], pair[1])
	}
	return nil, &armadaerrors.ErrMissingCredentials{
		AuthService: authService.Name(),
	}
}

func (authService *BasicAuthService) loginUser(username string, password string) (Principal, error) {
	userInfo, ok := authService.users[username]
	if ok && userInfo.Password == password {
		return NewStaticPrincipal(username, userInfo.Groups), nil
	}
	return nil, &armadaerrors.ErrInvalidCredentials{
		Username:    username,
		AuthService: authService.Name(),
	}
}
