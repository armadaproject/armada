package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/armada/authorization"
	"github.com/G-Research/armada/internal/armada/authorization/permissions"
)

func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permissions.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return status.Errorf(codes.PermissionDenied, "User have no permission: %s", permission)
	}
	return nil
}
