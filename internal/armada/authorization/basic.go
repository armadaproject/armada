package authorization

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/G-Research/armada/internal/common"
)

type BasicAuthService struct {
	users map[string]string
}

func NewBasicAuthService(users map[string]string) *BasicAuthService {
	return &BasicAuthService{users: users}
}

func (authService *BasicAuthService) Authenticate(ctx context.Context) (Principal, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[common.UsernameField]) <= 0 || len(md[common.PasswordField]) <= 0 {
			return nil, missingCredentials
		}

		userName := md[common.UsernameField][0]

		if !authService.isValidUser(userName, md[common.PasswordField][0]) {
			return nil, invalidCredentials
		}

		return NewStaticPrincipal(userName, []string{}), nil
	}

	return nil, missingCredentials
}

func (authService *BasicAuthService) isValidUser(username string, password string) bool {
	val, ok := authService.users[username]
	return ok && val == password
}
