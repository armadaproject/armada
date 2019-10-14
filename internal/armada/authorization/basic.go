package authorization

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/G-Research/k8s-batch/internal/common"
)

type BasicAuthorizeService struct {
	users map[string]string
}

func NewBasicAuthorizeService(users map[string]string) *BasicAuthorizeService {
	return &BasicAuthorizeService{users: users}
}

func (authService *BasicAuthorizeService) Authorize(ctx context.Context) (Principal, error) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[common.UsernameField]) <= 0 || len(md[common.PasswordField]) <= 0 {
			return nil, missingCredentials
		}

		userName := md[common.UsernameField][0]

		if !authService.isValidUser(userName, md[common.PasswordField][0]) {
			return nil, invalidCredentials
		}

		return NewSimplePrincipal(userName, []string{}), nil
	}

	return nil, missingCredentials
}

func (authService *BasicAuthorizeService) isValidUser(username string, password string) bool {
	val, ok := authService.users[username]
	return ok && val == password
}
