package authorization

import (
	"encoding/json"
	"github.com/armadaproject/armada/internal/common/context"
	"strings"

	"github.com/coreos/go-oidc"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
	"github.com/armadaproject/armada/internal/common/auth/permission"
)

type PermissionClaimQueries map[permission.Permission]string

type OpenIdAuthService struct {
	verifier    *oidc.IDTokenVerifier
	groupsClaim string
}

func NewOpenIdAuthServiceForProvider(ctx *context.ArmadaContext, config *configuration.OpenIdAuthenticationConfig) (*OpenIdAuthService, error) {
	provider, err := oidc.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}

	verifier := provider.Verifier(&oidc.Config{
		SkipClientIDCheck: config.SkipClientIDCheck,
		ClientID:          config.ClientId,
	})
	return NewOpenIdAuthService(verifier, config.GroupsClaim), nil
}

func NewOpenIdAuthService(verifier *oidc.IDTokenVerifier, groupsClaim string) *OpenIdAuthService {
	return &OpenIdAuthService{verifier, groupsClaim}
}

func (authService *OpenIdAuthService) Name() string {
	return "OIDC"
}

func (authService *OpenIdAuthService) Authenticate(ctx *context.ArmadaContext) (Principal, error) {
	token, err := grpc_auth.AuthFromMD(ctx, "bearer")
	if err != nil {
		return nil, &armadaerrors.ErrMissingCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	verifiedToken, err := authService.verifier.Verify(ctx, token)
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	rawClaims, err := extractRawClaims(verifiedToken)
	if err != nil {
		return nil, &armadaerrors.ErrInvalidCredentials{
			AuthService: authService.Name(),
			Message:     err.Error(),
		}
	}

	return NewStaticPrincipalWithScopesAndClaims(
		verifiedToken.Subject,
		authService.extractGroups(rawClaims),
		authService.extractScopes(verifiedToken),
		authService.extractClaims(rawClaims)), nil
}

func (authService *OpenIdAuthService) extractGroups(rawClaims map[string]*json.RawMessage) []string {
	rawGroups, ok := rawClaims[authService.groupsClaim]
	if !ok {
		return []string{}
	}

	groups := []string{}
	err := json.Unmarshal(*rawGroups, &groups)
	if err != nil {
		return []string{}
	}
	return groups
}

func (authService *OpenIdAuthService) extractScopes(token *oidc.IDToken) []string {
	scopeClaim := struct {
		Scope string `json:"scope"`
	}{}
	err := token.Claims(&scopeClaim)
	if err != nil {
		return []string{}
	}
	return strings.Split(scopeClaim.Scope, " ")
}

func (authService *OpenIdAuthService) extractClaims(rawClaims map[string]*json.RawMessage) []string {
	claims := []string{}
	for key := range rawClaims {
		claims = append(claims, key)
	}
	return claims
}

func extractRawClaims(token *oidc.IDToken) (map[string]*json.RawMessage, error) {
	rawClaims := map[string]*json.RawMessage{}
	err := token.Claims(&rawClaims)
	if err != nil {
		return nil, err
	}
	return rawClaims, nil
}
