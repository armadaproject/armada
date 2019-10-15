package authorization

import (
	"context"
	"encoding/json"

	"github.com/coreos/go-oidc"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/G-Research/k8s-batch/internal/armada/configuration"
)

type PermissionClaimQueries map[Permission]string

type OpenIdAuthService struct {
	verifier    *oidc.IDTokenVerifier
	groupsClaim string
}

func NewOpenIdAuthServiceForProvider(ctx context.Context, config *configuration.OpenIdAuthenticationConfig) (*OpenIdAuthService, error) {
	provider, err := oidc.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: config.ClientId})
	return NewOpenIdAuthService(verifier, config.GroupsClaim), nil
}

func NewOpenIdAuthService(verifier *oidc.IDTokenVerifier, groupsClaim string) *OpenIdAuthService {
	return &OpenIdAuthService{verifier, groupsClaim}
}

func (authService *OpenIdAuthService) Authenticate(ctx context.Context) (Principal, error) {

	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, missingCredentials
	}

	verifiedToken, err := authService.verifier.Verify(ctx, token)
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, err.Error())
	}

	return NewStaticPrincipal(verifiedToken.Subject, authService.extractGroups(verifiedToken)), nil
}

func (authService *OpenIdAuthService) extractGroups(token *oidc.IDToken) []string {
	rawClaims := map[string]*json.RawMessage{}
	err := token.Claims(&rawClaims)
	if err != nil {
		return []string{}
	}

	rawGroups, ok := rawClaims[authService.groupsClaim]
	if !ok {
		return []string{}
	}

	groups := []string{}
	err = json.Unmarshal(*rawGroups, &groups)
	if err != nil {
		return []string{}
	}
	return groups
}
