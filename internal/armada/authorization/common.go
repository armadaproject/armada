package authorization

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/util"
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

type StaticPrincipal struct {
	name   string
	groups map[string]bool
}

func NewStaticPrincipal(name string, groups []string) *StaticPrincipal {
	return &StaticPrincipal{
		name,
		util.StringListToSet(groups),
	}
}

func (p *StaticPrincipal) IsInGroup(group string) bool {
	return p.groups[group]
}

func (p *StaticPrincipal) GetName() string {
	return p.name
}

func GetPrincipal(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return NewStaticPrincipal("anonymous", []string{})
	}
	return p
}

func withPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

type AuthService interface {
	Authenticate(ctx context.Context) (Principal, error)
}

func CreateMiddlewareAuthFunction(authServices []AuthService) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		for _, service := range authServices {
			principal, err := service.Authenticate(ctx)
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
