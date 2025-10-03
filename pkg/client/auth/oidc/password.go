package oidc

import (
	"context"

	openId "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type ClientPasswordDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
	Username    string
	Password    string
}

func AuthenticateWithPassword(config ClientPasswordDetails, cacheToken bool) (*TokenCredentials, error) {
	ctx := context.Background()

	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}

	authConfig := &oauth2.Config{
		ClientID: config.ClientId,
		Scopes:   config.Scopes,
		Endpoint: provider.Endpoint(),
	}

	// Try to use cached refresh token if enabled
	token, cache, err := TryGetCachedToken(ctx, authConfig, config.ProviderUrl, config.ClientId, cacheToken)
	if err == nil && token != nil {
		return &TokenCredentials{oauth2.ReuseTokenSource(token, &FunctionTokenSource{
			func() (*oauth2.Token, error) {
				return authConfig.PasswordCredentialsToken(ctx, config.Username, config.Password)
			},
		})}, nil
	}

	source := &FunctionTokenSource{
		func() (*oauth2.Token, error) {
			return authConfig.PasswordCredentialsToken(ctx, config.Username, config.Password)
		},
	}
	t, err := source.Token()
	if err != nil {
		return nil, err
	}

	SaveTokenToCache(t, cache)
	cachedSource := oauth2.ReuseTokenSource(t, source)
	return &TokenCredentials{cachedSource}, nil
}
