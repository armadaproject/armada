package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	openId "github.com/coreos/go-oidc/v3/oidc"
	"golang.org/x/oauth2"
)

type DeviceDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
}

func AuthenticateDevice(config DeviceDetails) (*TokenCredentials, error) {
	ctx := context.Background()

	httpClient := http.DefaultClient

	provider, err := openId.NewProvider(ctx, config.ProviderUrl)
	if err != nil {
		return nil, fmt.Errorf("oidc discovery failed: %w", err)
	}

	// Get device authorization endpoint from discovery
	var claims struct {
		DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
	}
	if err := provider.Claims(&claims); err != nil {
		return nil, fmt.Errorf("decode discovery: %w", err)
	}
	if claims.DeviceAuthorizationEndpoint == "" {
		return nil, errors.New("provider does not advertise device_authorization_endpoint")
	}

	// Ensure "openid" scope is present
	scopes := make([]string, 0, len(config.Scopes)+1)
	seenOpenID := false
	for _, s := range config.Scopes {
		if s == openId.ScopeOpenID {
			seenOpenID = true
			break
		}
	}
	scopes = append(scopes, config.Scopes...)
	if !seenOpenID {
		scopes = append(scopes, openId.ScopeOpenID)
	}

	oauth := oauth2.Config{
		ClientID: config.ClientId,
		Endpoint: provider.Endpoint(),
		Scopes:   scopes,
	}

	deviceFlowResponse, err := requestDeviceAuthorization(ctx, httpClient, claims.DeviceAuthorizationEndpoint, config.ClientId, scopes)
	if err != nil {
		return nil, err
	}

	if deviceFlowResponse.VerificationURIComplete != "" {
		fmt.Printf("Complete your login in the browser:\n\n    %s\n", deviceFlowResponse.VerificationURIComplete)
		if cmd, err := openBrowser(deviceFlowResponse.VerificationURIComplete); err == nil && cmd != nil {
			defer func() { _ = cmd.Process.Kill() }()
		}
	} else {
		fmt.Printf("Go to:\n\n    %s\n\nand enter code: %s\n", deviceFlowResponse.VerificationURI, deviceFlowResponse.UserCode)
		if cmd, err := openBrowser(deviceFlowResponse.VerificationURI); err == nil && cmd != nil {
			defer func() { _ = cmd.Process.Kill() }()
		}
	}

	// Poll for token
	interval := deviceFlowResponse.Interval
	if interval <= 0 {
		interval = 5
	}
	expireAt := time.Now().Add(time.Duration(deviceFlowResponse.ExpiresIn) * time.Second)
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			token, err := requestToken(ctx, httpClient, oauth.Endpoint.TokenURL, config.ClientId, deviceFlowResponse.DeviceCode)
			if err == nil {
				fmt.Printf("\nAuthentication successful!\n\n")
				return &TokenCredentials{oauth.TokenSource(ctx, token)}, nil
			}

			var oe *oauthErr
			if errors.As(err, &oe) {
				switch oe.Code {
				case authorizationPending:
					// keep polling
				case slowDown:
					// cap at 15 seconds to avoid excessive delays
					maxPollingInterval := 15
					delay := 2
					jitter := rand.Intn(3) // 0-2 second jitter
					interval = min(interval+delay+jitter, maxPollingInterval)
					ticker.Reset(time.Duration(interval) * time.Second)
				case expiredToken, accessDenied:
					return nil, fmt.Errorf("%s: %s", oe.Code, oe.Description)
				default:
					return nil, fmt.Errorf("%s: %s", oe.Code, oe.Description)
				}
			} else {
				return nil, err
			}

			if time.Now().After(expireAt) {
				return nil, errors.New("device flow expired; please start again")
			}
		}
	}
}

type deviceFlowResponse struct {
	DeviceCode              string `json:"device_code"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
}

type oauthErrorResponse struct {
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
	ErrorURI         string `json:"error_uri"`
}

const (
	authorizationPending = "authorization_pending"
	expiredToken         = "expired_token"
	slowDown             = "slow_down"
	accessDenied         = "access_denied"
)

type oauthErr struct {
	Code        string
	Description string
	URI         string
}

func (e *oauthErr) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Description)
}

func requestDeviceAuthorization(ctx context.Context, c *http.Client, endpoint, clientID string, scopes []string) (*deviceFlowResponse, error) {
	form := url.Values{
		"client_id": {clientID},
		"scope":     {strings.Join(scopes, " ")},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, makeErrorForHTTPResponse(resp)
	}

	var deviceFlowResponse deviceFlowResponse
	if err := json.NewDecoder(resp.Body).Decode(&deviceFlowResponse); err != nil {
		return nil, err
	}
	return &deviceFlowResponse, nil
}

func requestToken(ctx context.Context, c *http.Client, tokenEndpoint, clientID, deviceCode string) (*oauth2.Token, error) {
	form := url.Values{
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"device_code": {deviceCode},
		"client_id":   {clientID},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := c.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var token oauth2.Token
		if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
			return nil, err
		}
		return &token, nil
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		// try to parse error response as oauthErrorResponse, otherwise return raw body
		b, _ := io.ReadAll(resp.Body)
		var oe oauthErrorResponse
		_ = json.Unmarshal(b, &oe)
		if oe.Error != "" {
			return nil, &oauthErr{Code: oe.Error, Description: oe.ErrorDescription, URI: oe.ErrorURI}
		}
		return nil, fmt.Errorf("oauth error: %s", strings.TrimSpace(string(b)))
	default:
		return nil, makeErrorForHTTPResponse(resp)
	}
}

func makeErrorForHTTPResponse(resp *http.Response) error {
	bodyBytes, _ := io.ReadAll(resp.Body)
	safeURL := sanitize(resp.Request.URL.Redacted())
	return fmt.Errorf("%s %s returned %s\n\n%q", resp.Request.Method, safeURL, resp.Status, bodyBytes)
}

func sanitize(str string) string {
	safeStr := strings.ReplaceAll(str, "\n", "")
	return strings.ReplaceAll(safeStr, "\r", "")
}
