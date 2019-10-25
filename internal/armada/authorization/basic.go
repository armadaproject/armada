package authorization

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
)

type BasicAuthService struct {
	users map[string]configuration.UserInfo
}

func NewBasicAuthService(users map[string]configuration.UserInfo) *BasicAuthService {
	return &BasicAuthService{users: users}
}

func (authService *BasicAuthService) Authenticate(ctx context.Context) (Principal, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[common.UsernameField]) <= 0 || len(md[common.PasswordField]) <= 0 {
			return nil, missingCredentials
		}

		userName := md[common.UsernameField][0]

		user, err := authService.loginUser(userName, md[common.PasswordField][0])
		if err != nil {
			return nil, err
		}

		return NewStaticPrincipal(userName, user.Groups), nil
	}

	return nil, missingCredentials
}

func (authService *BasicAuthService) loginUser(username string, password string) (*configuration.UserInfo, error) {
	val, ok := authService.users[username]
	if ok && val.Password == password {
		return &val, nil
	}
	return nil, invalidCredentials
}
