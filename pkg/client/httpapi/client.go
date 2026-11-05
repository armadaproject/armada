// Package httpapi is a client for an HTTP API that is authenticated with the credentials of an Armada connection,
// the way the gRPC connection is. The API can be any: Lookout, or the REST gateway of the Armada server.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/pkg/errors"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/pkg/client"
)

// Client sends requests to one HTTP API. Its credentials are resolved when it is made, and every request carries
// them. Resolving them can log in, which may be interactive, and a command makes several requests.
type Client struct {
	baseUrl     string
	authHeaders map[string]string
	httpClient  *http.Client
}

// New makes a client for the API at url, which is authenticated with the credentials of the connection. A url
// without a scheme gets http if it is local and https otherwise. Credentials are never sent over plain http to a
// remote host unless the connection has ForceNoTls set, whether in a request or in a redirect.
func New(connectionDetails *client.ApiConnectionDetails, url string) (*Client, error) {
	return newClient(connectionDetails, url, connectionDetails.AuthHeaders)
}

// newClient is New with the way the credentials are resolved given, which is a function of the url they are for.
func newClient(
	connectionDetails *client.ApiConnectionDetails,
	url string,
	resolveAuthHeaders func(ctx context.Context, url string) (map[string]string, error),
) (*Client, error) {
	if url == "" {
		return nil, errors.New("no url given for the http api")
	}
	base := baseUrl(url)

	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()
	headers, err := resolveAuthHeaders(ctx, base)
	if err != nil {
		return nil, err
	}
	return &Client{
		baseUrl:     base,
		authHeaders: headers,
		httpClient:  &http.Client{CheckRedirect: checkRedirect(connectionDetails, len(headers) > 0)},
	}, nil
}

// baseUrl adds a scheme to a url that has none: http for a local address, https otherwise.
func baseUrl(url string) string {
	url = strings.TrimRight(url, "/")
	if strings.Contains(url, "://") {
		return url
	}
	if client.IsLocalAddress(url) {
		return "http://" + url
	}
	return "https://" + url
}

// checkRedirect stops a request that carries credentials from following a redirect to somewhere they must not go.
// The credentials were only checked against the url the request started at, and a redirect to the same host
// keeps them, so an API that redirects from https to http would otherwise get them in clear text.
func checkRedirect(connectionDetails *client.ApiConnectionDetails, authenticated bool) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		if authenticated && !connectionDetails.AllowsCredentialsOver(req.URL.String()) {
			return fmt.Errorf("refusing to follow a redirect to %s: the credentials would be sent over plain http", req.URL.Redacted())
		}
		return nil
	}
}

// Get sends a GET request to path and decodes the JSON response into out.
func (c *Client) Get(path string, out any) error {
	return c.Do(http.MethodGet, path, nil, out)
}

// Post sends body as JSON to path and decodes the JSON response into out.
func (c *Client) Post(path string, body, out any) error {
	return c.Do(http.MethodPost, path, body, out)
}

// Do sends a request to path, with body as its JSON when there is one, and decodes the JSON response into out,
// unless out is nil. Any status but 2xx is an error that has the status and the start of the response.
func (c *Client) Do(method, path string, body, out any) error {
	ctx, cancel := common.ContextWithDefaultTimeout()
	defer cancel()

	var payload io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		payload = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseUrl+path, payload)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range c.authHeaders {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s %s returned %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
