package service

import (
	"context"
	"github.com/G-Research/k8s-batch/internal/common"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	missingCredentials = status.Errorf(codes.InvalidArgument, "missing credentials")
	invalidCredentials = status.Errorf(codes.Unauthenticated, "invalid username/password")
)

type AuthorizeService interface {
	Authorize(ctx context.Context) error
}

type BasicAuthAuthorizeService struct {
	users map[string]string
}

func (authService BasicAuthAuthorizeService) Authorize(ctx context.Context) error {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if len(md[common.UsernameField]) <= 0 || len(md[common.PasswordField]) <= 0 {
			return invalidCredentials
		}

		if !authService.isValidUser(md[common.UsernameField][0], md[common.PasswordField][0]) {
			return invalidCredentials
		}

		return nil
	}

	return missingCredentials
}

func (authService BasicAuthAuthorizeService) isValidUser(username string, password string) bool {
	val, ok := authService.users[username]
	return ok && val == password
}
