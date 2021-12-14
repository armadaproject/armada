package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
)

// Called by the Armada server to check permissions for gRPC calls.
//
// TODO We don't need this function.
func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		principal := authorization.GetPrincipal(ctx)
		name := principal.GetName()
		return status.Errorf(codes.PermissionDenied, "User %q does not have permission %q", name, permission)
	}
	return nil
}
