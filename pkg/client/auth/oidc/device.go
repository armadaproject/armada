package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	openId "github.com/coreos/go-oidc"
	"golang.org/x/oauth2"

	"github.com/armadaproject/armada/internal/common/logging"
)

type DeviceDetails struct {
	ProviderUrl string
	ClientId    string
	Scopes      []string
}

func AuthenticateDevice(config DeviceDetails) (*TokenCredentials, error) {
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

	c := &http.Client{}
	deviceFlowResponse, err := requestDeviceAuthorization(c, config)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Complete your login via OIDC. Launching browser to:\n\n    %s\n\n\n", deviceFlowResponse.VerificationUriComplete)

	if cmd, err := openBrowser(deviceFlowResponse.VerificationUriComplete); err != nil {
		fmt.Printf("Error attempting to automatically open browser: '%s'.\nPlease visit the above URL manually.\n", err)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := cmd.Process.Kill(); err != nil {
				fmt.Printf("Error killing your process: %s", err)
			}
		}()
	}

	for {
		time.Sleep(time.Duration(deviceFlowResponse.Interval) * time.Second)

		token, err := requestToken(c, config, deviceFlowResponse.DeviceCode)
		if err == nil {
			return &TokenCredentials{oauth.TokenSource(ctx, token)}, nil
		} else if err.Error() == authorizationPending {
			continue
		} else if err.Error() == slowDown {
			time.Sleep(time.Duration(deviceFlowResponse.Interval) * time.Second)
			continue
		} else if err.Error() == expiredToken {
			return nil, errors.New("token expired, please login again")
		} else {
			return nil, err
		}
	}
}

type deviceFlowResponse struct {
	DeviceCode              string `json:"device_code"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
	UserCode                string `json:"user_code"`
	VerificationUri         string `json:"verification_uri"`
	VerificationUriComplete string `json:"verification_uri_complete"`
}

type oauthErrorResponse struct {
	Error string `json:"error"`
}

var (
	authorizationPending = "authorization_pending"
	expiredToken         = "expiredtoken"
	slowDown             = "slow_down"
)

func requestDeviceAuthorization(c *http.Client, config DeviceDetails) (*deviceFlowResponse, error) {
	resp, err := c.PostForm(config.ProviderUrl+"/connect/deviceauthorization",
		url.Values{
			"client_id": {config.ClientId},
			"scope":     {strings.Join(config.Scopes, " ")},
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, makeErrorForHTTPResponse(resp)
	}

	var deviceFlowResponse deviceFlowResponse
	if err := json.NewDecoder(resp.Body).Decode(&deviceFlowResponse); err != nil {
		return nil, err
	}
	return &deviceFlowResponse, nil
}

func requestToken(c *http.Client, config DeviceDetails, deviceCode string) (*oauth2.Token, error) {
	resp, err := c.PostForm(config.ProviderUrl+"/connect/token",
		url.Values{
			"client_id":   {config.ClientId},
			"device_code": {deviceCode},
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		},
	)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 {
		var token oauth2.Token
		err = json.NewDecoder(resp.Body).Decode(&token)
		if err != nil {
			return nil, err
		}
		return &token, nil
	} else if resp.StatusCode == 400 {
		var errResp oauthErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
			return nil, err
		}
		return nil, errors.New(errResp.Error)
	}
	return nil, makeErrorForHTTPResponse(resp)
}

func makeErrorForHTTPResponse(resp *http.Response) error {
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	safeURL := logging.SanitizeUserInput(resp.Request.URL.String())
	return fmt.Errorf("%s %s returned HTTP %s; \n\n %#q", resp.Request.Method, safeURL, resp.Status, bodyBytes)
}
