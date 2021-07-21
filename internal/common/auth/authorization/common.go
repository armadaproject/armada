package authorization

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/util"
)

var (
	missingCredentials = status.Errorf(codes.InvalidArgument, "missing credentials")
	invalidCredentials = status.Errorf(codes.Unauthenticated, "invalid username/password")
)

const principalKey = "principal"

const EveryoneGroup = "everyone"

var anonymousPrincipal = NewStaticPrincipal("anonymous", []string{})

type Principal interface {
	GetName() string
	GetGroupNames() []string
	IsInGroup(group string) bool
	HasScope(scope string) bool
	HasClaim(claim string) bool
}

type StaticPrincipal struct {
	name   string
	groups map[string]bool
	scopes map[string]bool
	claims map[string]bool
}

func NewStaticPrincipal(name string, groups []string) *StaticPrincipal {
	return &StaticPrincipal{
		name,
		util.StringListToSet(append(groups, EveryoneGroup)),
		map[string]bool{},
		map[string]bool{},
	}
}

func NewStaticPrincipalWithScopesAndClaims(name string, groups []string, scopes []string, claims []string) *StaticPrincipal {
	return &StaticPrincipal{
		name,
		util.StringListToSet(append(groups, EveryoneGroup)),
		util.StringListToSet(scopes),
		util.StringListToSet(claims),
	}
}

func (p *StaticPrincipal) IsInGroup(group string) bool {
	return p.groups[group]
}

func (p *StaticPrincipal) HasScope(scope string) bool {
	return p.scopes[scope]
}

func (p *StaticPrincipal) HasClaim(claim string) bool {
	return p.claims[claim]
}

func (p *StaticPrincipal) GetName() string {
	return p.name
}

func (p *StaticPrincipal) GetGroupNames() []string {
	names := []string{}
	for g := range p.groups {
		names = append(names, g)
	}
	return names
}

func GetPrincipal(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return anonymousPrincipal
	}
	return p
}

func WithPrincipal(ctx context.Context, principal Principal) context.Context {
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
			// record user name for request logging
			grpc_ctxtags.Extract(ctx).Set("user", principal.GetName())
			return WithPrincipal(ctx, principal), nil
		}
		return nil, status.Errorf(codes.Unauthenticated, "Request in not authenticated with any of the supported schemes.")
	}
}
