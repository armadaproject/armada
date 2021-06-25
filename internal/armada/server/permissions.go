package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
)

func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		return status.Errorf(codes.PermissionDenied, "User have no permission: %s", permission)
	}
	return nil
}
