package auth

import (
	"context"
	"net/http"

	"github.com/grpc-ecosystem/go-grpc-middleware/v2/metadata"
	"golang.org/x/exp/slices"

	"github.com/armadaproject/armada/internal/common/util"
)

// Name of the key used to store principals in contexts.
const principalKey = "principal"

// All users are implicitly part of this group.
const EveryoneGroup = "everyone"

// Principal represents an entity that can be authenticated (e.g., a user).
// Each principal has a name associated with it and may be part of one or more groups.
// Scopes and claims are as defined in OpenId.
type Principal interface {
	GetName() string
	GetAuthMethod() string
	GetGroupNames() []string
	IsInGroup(group string) bool
	HasScope(scope string) bool
	HasClaim(claim string) bool
}

// Default implementation of the Principal interface.
// Here, static refers to the fact that the principal doesn't change once it has been created.
type StaticPrincipal struct {
	name       string
	authMethod string
	groups     map[string]bool
	scopes     map[string]bool
	claims     map[string]bool
}

func NewStaticPrincipal(name string, authMethod string, groups []string) *StaticPrincipal {
	return &StaticPrincipal{
		name,
		authMethod,
		util.StringListToSet(append(groups, EveryoneGroup)),
		map[string]bool{},
		map[string]bool{},
	}
}

func NewStaticPrincipalWithScopesAndClaims(name string, authMethod string, groups []string, scopes []string, claims []string) *StaticPrincipal {
	return &StaticPrincipal{
		name,
		authMethod,
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

func (p *StaticPrincipal) GetAuthMethod() string {
	return p.authMethod
}

func (p *StaticPrincipal) GetGroupNames() []string {
	names := []string{}
	for g := range p.groups {
		names = append(names, g)
	}
	slices.Sort(names) // sort names so that we have stable output for testing
	return names
}

// GetPrincipal returns the principal (e.g., a user) contained in a context.
// The principal is assumed to be stored as a ctx.Value.
// If no principal can be found, a principal representing an anonymous (unauthenticated) user is returned.
func GetPrincipal(ctx context.Context) Principal {
	p, ok := ctx.Value(principalKey).(Principal)
	if !ok {
		return anonymousPrincipal
	}
	return p
}

// WithPrincipal returns a new context containing a principal that is a child to the given context.
func WithPrincipal(ctx context.Context, principal Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

// AuthService represents a method of authentication for the HTTP or gRPC API.
// Each implementation represents a particular method, e.g., username/password or OpenID.
// The HTTP/gRPC server may be started with multiple AuthService to give several options for authentication.
type AuthService interface {
	Authenticate(ctx context.Context, authHeader string) (Principal, error)
}

// CreateGrpcMiddlewareAuthFunction for use with GRPC.
// That function returns success if any service successfully
// authenticates the user, and an error if all services fail to authenticate.
//
// If authentication succeeds, the username returned by the authentication service is added to the
// request context for logging purposes.
func CreateGrpcMiddlewareAuthFunction(authService AuthService) func(ctx context.Context) (context.Context, error) {
	return func(ctx context.Context) (context.Context, error) {
		authHeader := metadata.ExtractIncoming(ctx).Get("authorization")
		principal, err := authService.Authenticate(ctx, authHeader)
		if err != nil {
			return nil, err
		}
		return WithPrincipal(ctx, principal), nil
	}
}

// CreateHttpMiddlewareAuthFunction for use with GRPC.
// That function returns success if any service successfully
// authenticates the user, and an error if all services fail to authenticate.
//
// If authentication succeeds, the username returned by the authentication service is added to the
// request context for logging purposes.
func CreateHttpMiddlewareAuthFunction(authService AuthService) func(w http.ResponseWriter, r *http.Request) (context.Context, error) {
	return func(w http.ResponseWriter, r *http.Request) (context.Context, error) {
		ctx := r.Context()

		authHeader := r.Header.Get("Authorization")
		principal, err := authService.Authenticate(ctx, authHeader)
		if err != nil {
			http.Error(w, "auth error:"+err.Error(), http.StatusInternalServerError)
			return nil, err
		}
		return WithPrincipal(ctx, principal), nil
	}
}
