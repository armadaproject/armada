package server

// TODO This file should be called "authentication.go"

import (
	"context"
	"fmt"

	"github.com/G-Research/armada/internal/common/auth/authorization"
	"github.com/G-Research/armada/internal/common/auth/permission"
)

// ErrNoPermission represents an error that occurs when a client tries to perform some action
// through the gRPC API for which it does not have permissions. The error contains the name
// of the client and the requested permission.
type ErrNoPermission struct {
	UserName   string
	Permission permission.Permission
}

func (err *ErrNoPermission) Error() string {
	return fmt.Sprintf("user %q lacks permission %q", err.UserName, err.Permission)
}

// checkPermission is a helper function called by the gRPC handlers to check if a client has the
// permissions required to perform some action. The error returned is of type ErrNoPermission.
// After recovering the error (using errors.As), the caller can obtain the name of the user and the
// requested permission programatically via this error type.
func checkPermission(p authorization.PermissionChecker, ctx context.Context, permission permission.Permission) error {
	if !p.UserHasPermission(ctx, permission) {
		principal := authorization.GetPrincipal(ctx)
		name := principal.GetName()
		return &ErrNoPermission{UserName: name, Permission: permission}
	}
	return nil
}
