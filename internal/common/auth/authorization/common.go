package authorization

import (
	"context"
	"fmt"
	"strings"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/armada/internal/common/util"
)

var (
	missingCredentialsErr = status.Errorf(codes.InvalidArgument, "missing credentials")
	invalidCredentialsErr = status.Errorf(codes.Unauthenticated, "invalid username/password")
)

// Name of the key used to store principals in contexts.
const principalKey = "principal"

// All users are implicitly part of this group.
const EveryoneGroup = "everyone"

// Default principal used if no principal can be found in a context.
var anonymousPrincipal = NewStaticPrincipal("anonymous", []string{})

// Principal represents an entity that can be authenticated (e.g., a user).
// Each principal has a name associated with it and may be part of one or more groups.
// Scopes and claims are as defined in OpenId.
type Principal interface {
	GetName() string
	GetGroupNames() []string
	IsInGroup(group string) bool
	HasScope(scope string) bool
	HasClaim(claim string) bool
}

// Default implementation of the Principal interface.
// Here, static refers to the fact that the principal doesn't change once it has been created.
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

// AuthService represents a method of authentication for the gRPC API.
// Each implementation represents a particular method, e.g., username/password or OpenID.
// The gRPC server may be started with multiple AuthService to give several options for authentication.
type AuthService interface {
	Authenticate(ctx context.Context) (Principal, error)
	Name() string
}

type authAttempt struct {
	PrincipalName   string
	AuthServiceName string
	Error           error
}

func (aa *authAttempt) String() string {
	return fmt.Sprintf("Auth attempted with principal %q via service %q, encountered error: %q",
		aa.PrincipalName,
		aa.AuthServiceName,
		aa.Error.Error())
}

// CreateMiddlewareAuthFunction returns an authentication function that combines the given
// authentication services. That function returns success if any service successfully
// authenticates the user, and an error if all services fail to authenticate.
// The services in authServices are tried one at a time in sequence.
// Successful authentication short-circuits the process.
//
// If authentication succeeds, the username returned by the authentication service is added to the
// request context for logging purposes.
func CreateMiddlewareAuthFunction(authServices []AuthService) grpc_auth.AuthFunc {
	return func(ctx context.Context) (context.Context, error) {
		authAttempts := make([]*authAttempt, 0, len(authServices))
		for _, service := range authServices {
			principal, err := service.Authenticate(ctx)

			if err == missingCredentialsErr ||
				err == invalidCredentialsErr {
				principalName := ""
				if principal != nil {
					principalName = principal.GetName()
				}
				authAttempts = append(authAttempts, &authAttempt{
					PrincipalName:   principalName,
					AuthServiceName: service.Name(),
					Error:           err,
				})
				// try next auth service
				continue
			} else if err != nil {
				return nil, err
			}

			// record user name & auth service for request logging
			grpc_ctxtags.Extract(ctx).Set("user", principal.GetName())
			grpc_ctxtags.Extract(ctx).Set("authService", service.Name())
			return WithPrincipal(ctx, principal), nil
		}

		var attempts strings.Builder
		for _, attempt := range authAttempts {
			fmt.Fprintf(&attempts, "[%s] ", attempt.String())
		}
		return nil, status.Errorf(codes.Unauthenticated,
			"Request could not be authenticated with any of the supported schemes: %s", attempts.String())
	}
}
