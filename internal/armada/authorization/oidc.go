package authorization

import (
	"context"

	"github.com/coreos/go-oidc"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
)

type OpenIdAuthorizeService struct {
	verifier *oidc.IDTokenVerifier
}

func NewJwtAuthorizeService(verifier *oidc.IDTokenVerifier) *OpenIdAuthorizeService {
	return &OpenIdAuthorizeService{verifier}
}

func (authService *OpenIdAuthorizeService) Authorize(ctx context.Context) (Principal, error) {

	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, err
	}

	verifiedToken, err := authService.verifier.Verify(ctx, token)
	if err != nil {
		return nil, err // todo: better error
	}

	return &SimplePrincipal{
		verifiedToken.Subject,
		func(p Permission) bool {
			return authService.assertPermission(verifiedToken)
		},
	}, nil
}

func (authService *OpenIdAuthorizeService) assertPermission(token *oidc.IDToken) bool {

	var claims interface{}
	err := token.Claims(&claims)
	if err != nil {
		return false
	}

	//perms, err := jsonpath.Read(claims, "$..authors")

	return true
}
