package httpapi

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"
)

// credentialsTTL is how long the headers that authenticate requests are used before they are asked for again. A
// request asks the credentials for their current token, which they renew when it has expired, so a command that runs
// for a long time keeps working. Some credentials are expensive to ask, such as a command that is run each time, so
// the answer is kept for a short while.
//
// It must be well under the time before expiry at which a token is renewed, or a token could be sent after it has
// expired. The OIDC credentials renew a token ten seconds before it expires (oauth2's expiry delta), so a token is
// asked for with at least that long left, and kept for half of it at most.
//
// A token that the server rejects, whatever the reason, is asked for again at once: see authTransport.
const credentialsTTL = 5 * time.Second

// authTransport adds the credentials to every request to the host of the API, and to no other host. A redirect to
// another host is not sent them, whatever the other host is.
//
// A request that is answered with 401 Unauthorized, when the credentials were kept from an earlier request, is sent
// again once with the credentials asked for afresh: the token may have been revoked or may have expired, which the
// credentials did not know when they were kept. A request that is still not accepted is not sent again.
type authTransport struct {
	next         http.RoundTripper
	host         string
	authenticate func(ctx context.Context, url string) (map[string]string, error)
	ttl          time.Duration
	now          func() time.Time

	mu      sync.Mutex
	headers map[string]string
	fetched time.Time
}

func newAuthTransport(next http.RoundTripper, host string, authenticate func(ctx context.Context, url string) (map[string]string, error), headers map[string]string, o *options) *authTransport {
	return &authTransport{next: next, host: host, authenticate: authenticate, ttl: o.credentialsTTL, now: o.now, headers: headers, fetched: o.now()}
}

func (t *authTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if !strings.EqualFold(req.URL.Hostname(), t.host) {
		return t.next.RoundTrip(req)
	}
	headers, fresh, err := t.current(req, false)
	if err != nil {
		return nil, err
	}
	resp, err := t.send(req, req.Body, headers)
	if err != nil || resp.StatusCode != http.StatusUnauthorized || fresh {
		// Credentials that were just asked for are not asked for again: they would be the same.
		return resp, err
	}
	body, replayable := replayableBody(req)
	if !replayable {
		return resp, nil
	}

	refreshed, _, err := t.current(req, true)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("asking for new credentials after the request was not authorized: %w", err)
	}
	if maps.Equal(refreshed, headers) {
		// The same credentials cannot be expected to be accepted this time.
		return resp, nil
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	resp.Body.Close()
	return t.send(req, body(), refreshed)
}

// send sends a copy of the request with the headers and the body given. A round tripper must not change the request it
// is given.
func (t *authTransport) send(req *http.Request, body io.ReadCloser, headers map[string]string) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Body = body
	for k, v := range headers {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}
	return t.next.RoundTrip(req)
}

// replayableBody returns how to get the body of a request again, if it can be: it has none, or says how.
func replayableBody(req *http.Request) (body func() io.ReadCloser, ok bool) {
	if req.Body == nil || req.Body == http.NoBody {
		return func() io.ReadCloser { return http.NoBody }, true
	}
	if req.GetBody == nil {
		return nil, false
	}
	return func() io.ReadCloser {
		b, err := req.GetBody()
		if err != nil {
			return http.NoBody
		}
		return b
	}, true
}

// current returns the headers to authenticate the request with, and whether they were just asked for. They are asked
// for again when they are older than the ttl, or when force is set.
func (t *authTransport) current(req *http.Request, force bool) (headers map[string]string, fresh bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !force && t.headers != nil && t.now().Sub(t.fetched) < t.ttl {
		return t.headers, false, nil
	}
	headers, err = t.authenticate(req.Context(), req.URL.String())
	if err != nil {
		return nil, false, err
	}
	t.headers, t.fetched = headers, t.now()
	return headers, true, nil
}
