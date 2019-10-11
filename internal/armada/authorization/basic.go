package authorization

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/G-Research/k8s-batch/internal/common"
)

type BasicAuthAuthorizeService struct {
	users map[string]string
}

func NewBasicAuthAuthorizeService(users map[string]string) *BasicAuthAuthorizeService {
	return &BasicAuthAuthorizeService{users: users}
}

func (authService *BasicAuthAuthorizeService) Authorize(ctx context.Context) (Principal, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[common.UsernameField]) <= 0 || len(md[common.PasswordField]) <= 0 {
			return nil, invalidCredentials
		}

		userName := md[common.UsernameField][0]

		if !authService.isValidUser(userName, md[common.PasswordField][0]) {
			return nil, invalidCredentials
		}

		return &SimplePrincipal{
				userName,
				func(Permission) bool { return true }},
			nil
	}

	return nil, missingCredentials
}

func (authService *BasicAuthAuthorizeService) isValidUser(username string, password string) bool {
	val, ok := authService.users[username]
	return ok && val == password
}
