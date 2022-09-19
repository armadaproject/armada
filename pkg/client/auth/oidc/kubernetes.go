package oidc

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/oauth2"
)

type KubernetesDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
}

func AuthenticateKubernetes(config KubernetesDetails) (*TokenCredentials, error) {
	tokenSource := FunctionTokenSource{
		GetToken: func() (*oauth2.Token, error) {
			log.Info("Getting new authentication token")
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

			defer resp.Body.Close()

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

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return nil, err
			}

			token, err := parseOIDCToken(body)
			if err != nil {
				return nil, err
			}

			return token, nil
		},
	}

	return &TokenCredentials{TokenSource: oauth2.ReuseTokenSource(nil, &tokenSource)}, nil
}

/**
 * parseOIDCToken takes a JSON OIDC response and returns a correctly set OIDC token struct.
 *
 * See https://github.com/golang/oauth2/blob/8227340efae7cbdad9f68d6dff2b2c3306714564/jwt/jwt.go#L150
 */
func parseOIDCToken(body []byte) (*oauth2.Token, error) {
	// tokenRes is the JSON response body.
	var tokenRes struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int64  `json:"expires_in"` // relative seconds from now
	}
	if err := json.Unmarshal(body, &tokenRes); err != nil {
		return nil, fmt.Errorf("kubernetes flow: cannot fetch token: %v", err)
	}
	token := &oauth2.Token{
		AccessToken: tokenRes.AccessToken,
		TokenType:   tokenRes.TokenType,
	}
	if secs := tokenRes.ExpiresIn; secs > 0 {
		token.Expiry = time.Now().Add(time.Duration(secs) * time.Second)
	}
	if expiry, err := extractExpiry(tokenRes.AccessToken); err != nil {
		if token.Expiry.IsZero() || token.Expiry.After(*expiry) {
			token.Expiry = *expiry
		}
	}
	return token, nil
}

/**
 * extractExpiry retrieves the expiry time from the OIDC JWT
 *
 * A modified version of golang.org/x/oauth2/jws
 */
func extractExpiry(payload string) (*time.Time, error) {
	s := strings.Split(payload, ".")
	if len(s) < 2 {
		return nil, errors.New("kubernetes flow: invalid token received")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(s[1])
	if err != nil {
		return nil, err
	}
	var c struct {
		Exp int64 `json:"exp"`
	}
	err = json.Unmarshal(decoded, &c)
	if err != nil {
		return nil, err
	}
	if c.Exp == 0 {
		return nil, fmt.Errorf("kubernetes flow: expiration time in JWT shouldn't be 0")
	}
	ret := time.Unix(c.Exp, 0)
	return &ret, nil
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
