package authorization

import (
	"context"
	"encoding/base64"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"github.com/G-Research/armada/internal/common/auth/configuration"
)

type BasicAuthService struct {
	users map[string]configuration.UserInfo
}

func NewBasicAuthService(users map[string]configuration.UserInfo) *BasicAuthService {
	return &BasicAuthService{users: users}
}

func (authService *BasicAuthService) Authenticate(ctx context.Context) (Principal, error) {
	basicAuth, err := grpc_auth.AuthFromMD(ctx, "basic")
	if err == nil {
		payload, err := base64.StdEncoding.DecodeString(basicAuth)
		if err != nil {
			return nil, err
		}
		pair := strings.SplitN(string(payload), ":", 2)
		return authService.loginUser(pair[0], pair[1])
	}
	return nil, missingCredentials
}

func (authService *BasicAuthService) loginUser(username string, password string) (Principal, error) {
	userInfo, ok := authService.users[username]
	if ok && userInfo.Password == password {
		return NewStaticPrincipal(username, userInfo.Groups), nil
	}
	return nil, invalidCredentials
}
