package kubernetes

import (
	"context"

	"golang.org/x/oauth2"
)

type NativeTokenCredentials struct {
	TokenSource oauth2.TokenSource
}

func (c *NativeTokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	jwt, err := c.getJWT(c.TokenSource)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization": "KubernetesAuth " + jwt,
	}, nil
}

func (c *NativeTokenCredentials) RequireTransportSecurity() bool {
	return false
}

func (c *NativeTokenCredentials) getJWT(source oauth2.TokenSource) (string, error) {
	t, e := source.Token()
	if e != nil {
		return "", e
	}
	return t.AccessToken, nil
}
