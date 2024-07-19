package auth

import (
	"context"
	"encoding/base64"
	"strings"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

const BasicAuthServiceName = "Basic"

type BasicAuthService struct {
	users map[string]configuration.UserInfo
}

func NewBasicAuthService(users map[string]configuration.UserInfo) *BasicAuthService {
	return &BasicAuthService{users: users}
}

func (authService *BasicAuthService) Authenticate(_ context.Context, authHeader string) (Principal, error) {
	authHeaderSplits := strings.SplitN(authHeader, " ", 2)
	if len(authHeaderSplits) < 2 || !strings.EqualFold(authHeaderSplits[0], "basic") {
		return nil, &armadaerrors.ErrMissingCredentials{
			AuthService: BasicAuthServiceName,
			Message:     "basic auth header not found",
		}
	}

	payload, err := base64.StdEncoding.DecodeString(authHeaderSplits[1])
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: BasicAuthServiceName,
			Message:     err.Error(),
		}
	}
	pair := strings.SplitN(string(payload), ":", 2)
	return authService.loginUser(pair[0], pair[1])
}

func (authService *BasicAuthService) loginUser(username string, password string) (Principal, error) {
	userInfo, ok := authService.users[username]
	if ok && userInfo.Password == password {
		return NewStaticPrincipal(username, BasicAuthServiceName, userInfo.Groups), nil
	}
	return nil, &armadaerrors.ErrInvalidCredentials{
		Username:    username,
		AuthService: BasicAuthServiceName,
	}
}
