package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/authorization"
)

func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission authorization.Permission) error {
	if !p.UserHavePermission(ctx, permission) {
		return status.Errorf(codes.PermissionDenied, "User have no permission: %s", permission)
	}
	return nil
}
