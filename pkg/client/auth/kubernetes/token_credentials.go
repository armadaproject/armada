package kubernetes

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

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

	ca, err := getClusterCA()
	if err != nil {
		return nil, err
	}

	encodedCa := base64.RawURLEncoding.EncodeToString([]byte(ca))
	body := fmt.Sprintf(`{"token":"%s", "ca":"%s"}`, jwt, encodedCa)
	encoded := base64.RawURLEncoding.EncodeToString([]byte(body))

	return map[string]string{
		"authorization": "KubernetesAuth " + encoded,
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

func getClusterCA() (string, error) {
	fromFile, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
	if err != nil {
		return "", err
	}

	return string(fromFile), nil
}
