package oidc

import (
	"encoding/b64"
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2"
)

type KubernetesDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
}

func AuthenticateKubernetes(config KubernetesDetails) (*TokenCredentials, error) {
	tokenSource := functionTokenSource{
		getToken: func() (*oauth2.Token, error) {
			log.Println("Getting new token from IDS")
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

			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}

			var result map[string]interface{}
			json.Unmarshal(body, &result)

			accessToken, ok := result["access_token"]
			if !ok {
				return nil, fmt.Errorf("Unmarshalled yaml fails to contain access token")
			}

			tokenBody, ok := strings.Split(accessToken, ".")[1]
			if !ok {
				return nil, fmt.Errorf("Access token is incorrectly formatted, no body section")
			}

			decodedBody, err := b64.StdEncoding.DecodeString(tokenBody)
			if err != nil {
				return nil, err
			}

			var resultBody map[string]interface{}
			json.Unmarshal([]byte(decodedBody), resultBody)

			expiry, ok := resultBody["exp"]
			if !ok {
				return nil, fmt.Errorf("No expiry in token")
			}

			token := oauth2.Token{
				AccessToken: accessToken,
				TokenType:   "Bearer",
				Expiry:      time.Unix(expiry, 0),
			}

			log.Printf("Access Token: %v \n", token.AccessToken)
			log.Printf("Token Expiry: %v \n", token.Expiry)
			return &token, nil
		},
	}

	return &TokenCredentials{tokenSource: oauth2.ReuseTokenSource(nil, &tokenSource)}, nil
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
