package oidc

import (
	"context"

	"golang.org/x/oauth2"
)

type TokenCredentials struct {
	TokenSource oauth2.TokenSource
}

func (c *TokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	jwt, err := c.getJWT(c.TokenSource)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": "Bearer " + jwt,
	}, nil
}

func (c *TokenCredentials) RequireTransportSecurity() bool {
	return false
}

func (c *TokenCredentials) getJWT(source oauth2.TokenSource) (string, error) {
	t, e := source.Token()
	if e != nil {
		return "", e
	}
	return t.AccessToken, nil
}

type FunctionTokenSource struct {
	GetToken func() (*oauth2.Token, error)
}

func (f *FunctionTokenSource) Token() (*oauth2.Token, error) {
	return f.GetToken()
}
