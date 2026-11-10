package httpapi

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

// credentialsTTL is how long the headers that authenticate requests are used before they are asked for again. A
// request asks the credentials for their current token, which they renew when it has expired, so a command that runs
// for a long time keeps working. Some credentials are expensive to ask, such as a command that is run each time, so
// the answer is kept for a short while.
const credentialsTTL = 30 * time.Second

// authTransport adds the credentials to every request to the host of the API, and to no other host. A redirect to
// another host is not sent them, whatever the other host is.
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
	headers, err := t.current(req)
	if err != nil {
		return nil, err
	}
	// A round tripper must not change the request it is given.
	req = req.Clone(req.Context())
	for k, v := range headers {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}
	return t.next.RoundTrip(req)
}

// current returns the headers to authenticate the request with, asking the credentials for them again when they are
// older than the ttl.
func (t *authTransport) current(req *http.Request) (map[string]string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.headers != nil && t.now().Sub(t.fetched) < t.ttl {
		return t.headers, nil
	}
	headers, err := t.authenticate(req.Context(), req.URL.String())
	if err != nil {
		return nil, err
	}
	t.headers, t.fetched = headers, t.now()
	return headers, nil
}
