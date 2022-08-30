package oidc

import (
	"context"

	openId "github.com/coreos/go-oidc"
	"golang.org/x/oauth2/clientcredentials"
)

type ClientCredentialsDetails struct {
	ProviderUrl  string
	ClientId     string
	ClientSecret string
	Scopes       []string
}

func AuthenticateWithClientCredentials(config ClientCredentialsDetails) (*TokenCredentials, error) {
	ctx := context.Background()

	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}

	authConfig := &clientcredentials.Config{
		ClientID:     config.ClientId,
		ClientSecret: config.ClientSecret,
		Scopes:       config.Scopes,
		TokenURL:     provider.Endpoint().TokenURL,
	}

	source := authConfig.TokenSource(ctx)
	return &TokenCredentials{source}, nil
}
