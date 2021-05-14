package oidc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	openId "github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
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
		defer cmd.Process.Kill()
	}

	for {
		time.Sleep(time.Duration(deviceFlowResponse.Interval) * time.Second)
		token, err := oauth.Exchange(ctx, "",
			oauth2.SetAuthURLParam("client_id", config.ClientId),
			oauth2.SetAuthURLParam("device_code", deviceFlowResponse.DeviceCode),
			oauth2.SetAuthURLParam("grant_type", "urn:ietf:params:oauth:grant-type:device_code"))

		if err == nil {
			return &TokenCredentials{oauth.TokenSource(ctx, token)}, nil
		} else if err == keepPolling {
			continue
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

var keepPolling = errors.New("keep polling")

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
	err = json.NewDecoder(resp.Body).Decode(&deviceFlowResponse)
	if err != nil {
		return nil, err
	}
	return &deviceFlowResponse, nil
}

func makeErrorForHTTPResponse(resp *http.Response) error {
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		bodyBytes = []byte(err.Error())
	}
	return fmt.Errorf("%s %s returned HTTP %s; \n\n %#q", resp.Request.Method, resp.Request.URL, resp.Status, bodyBytes)
}
