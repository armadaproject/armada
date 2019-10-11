package authorization

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	missingCredentials = status.Errorf(codes.InvalidArgument, "missing credentials")
	invalidCredentials = status.Errorf(codes.Unauthenticated, "invalid username/password")
)

const principalKey = "principal"

type Permission string

type Principal interface {
	GetName() string
	HasPermission(perm Permission) bool
}

type SimplePrincipal struct {
	name          string
	hasPermission func(perm Permission) bool
}

func (p *SimplePrincipal) GetName() string {
	return p.name
}
func (p *SimplePrincipal) HasPermission(perm Permission) bool {
	return p.hasPermission(perm)
}

func GetPrincipal(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return &SimplePrincipal{
			"anonymous",
			func(Permission) bool { return false }}
	}
	return p
}

func CheckPermission(ctx context.Context, perm Permission) bool {
	principal := GetPrincipal(ctx)
	return principal.HasPermission(perm)
}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

type AuthorizeService interface {
	Authorize(ctx context.Context) (Principal, error)
}

func CreateMiddlewareAuthFunction(authService AuthorizeService) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		principal, err := authService.Authorize(ctx)
		if err != nil {
			return nil, err
		}
		return withPrincipal(ctx, principal), nil
	}
}
