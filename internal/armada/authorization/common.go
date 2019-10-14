package authorization

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/k8s-batch/internal/common/util"
)

var (
	missingCredentials = status.Errorf(codes.InvalidArgument, "missing credentials")
	invalidCredentials = status.Errorf(codes.Unauthenticated, "invalid username/password")
)

const principalKey = "principal"

type Principal interface {
	GetName() string
	IsInGroup(group string) bool
}

type SimplePrincipal struct {
	name   string
	groups map[string]bool
}

func NewSimplePrincipal(name string, groups []string) *SimplePrincipal {
	return &SimplePrincipal{
		name,
		util.StringListToSet(groups),
	}
}

func (p *SimplePrincipal) IsInGroup(group string) bool {
	return p.groups[group]
}

func (p *SimplePrincipal) GetName() string {
	return p.name
}

func GetPrincipal(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return NewSimplePrincipal("anonymous", []string{})
	}
	return p
}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

type AuthorizeService interface {
	Authorize(ctx context.Context) (Principal, error)
}

func CreateMiddlewareAuthFunction(authServices []AuthorizeService) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		for _, service := range authServices {
			principal, err := service.Authorize(ctx)
			if err == missingCredentials {
				// try next auth service
				continue
			}
			if err != nil {
				return nil, err
			}
			return withPrincipal(ctx, principal), nil
		}
		return nil, status.Errorf(codes.Unauthenticated, "Request in not authenticated with any of the supported schemes.")
	}
}
