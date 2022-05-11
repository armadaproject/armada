package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"os"
	"strings"

	openId "github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

type KubernetesDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
}

func AuthenticateKubernetes(config KubernetesDetails) (*TokenCredentials, error) {
	ctx := context.Background()
	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, err
	}

	oauth := oauth2.Config{
		ClientID: config.ClientId,
		Endpoint: provider.Endpoint(),
		Scopes:   append(config.Scopes, openId.ScopeOpenID),
	}

	kubernetesToken, err := getKubernetesToken()
	if err != nil {
		return nil, err
	}

	c := &http.Client{}
	resp, err := c.PostForm(config.ProviderUrl+"/connect/token",
		url.Values{
			"client_id":        {config.ClientId},
			"scope":            {strings.Join(config.Scopes, " ")},
			"grant_type":       {"kubernetes"},
			"client_assertion": {kubernetesToken},
		})

	if err != nil {
		return nil, err
	}

	if resp.StatusCode == 400 {
		var errResp oauthErrorResponse
		err = json.NewDecoder(resp.Body).Decode(&errResp)
		if err != nil {
			return nil, err
		}
		return nil, errors.New(errResp.Error)
	} else if resp.StatusCode != 200 {
		return nil, makeErrorForHTTPResponse(resp)
	}

	var token oauth2.Token
	err = json.NewDecoder(resp.Body).Decode(&token)
	if err != nil {
		return nil, err
	}

	return &TokenCredentials{tokenSource: oauth.TokenSource(ctx, &token)}, nil
}

func getKubernetesToken() (string, error) {
	fromEnv := os.Getenv("K8S_SERVICEACCOUNT_TOKEN")
	if fromEnv != "" {
		return fromEnv, nil
	}

	fromFile, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/token")
	if err != nil {
		return "", err
	}

	return string(fromFile), nil
}
