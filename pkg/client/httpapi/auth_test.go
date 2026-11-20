package httpapi

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// A round tripper must not change the request it is given, which the caller may use again.
func TestAuthTransport_DoesNotChangeTheRequestItIsGiven(t *testing.T) {
	var sent *http.Request
	next := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		sent = req
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	auth := &fakeAuth{}
	headers, _ := auth.lookup(nil, "")
	transport := newAuthTransport(next, "api.example.com", auth.lookup, headers, &options{now: time.Now, credentialsTTL: time.Minute})
	req, err := http.NewRequest(http.MethodGet, "https://api.example.com/a", nil)
	require.NoError(t, err)

	_, err = transport.RoundTrip(req)

	require.NoError(t, err)
	assert.Equal(t, "Bearer token-1", sent.Header.Get("authorization"))
	assert.Empty(t, req.Header.Get("authorization"), "the request the caller has is as it was")
}

// A header the caller set is the caller's.
func TestAuthTransport_LeavesAnAuthorizationTheRequestHasAlone(t *testing.T) {
	var sent *http.Request
	next := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		sent = req
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	})
	auth := &fakeAuth{}
	headers, _ := auth.lookup(nil, "")
	transport := newAuthTransport(next, "api.example.com", auth.lookup, headers, &options{now: time.Now, credentialsTTL: time.Minute})
	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com/a", nil)
	req.Header.Set("authorization", "Bearer mine")

	_, err := transport.RoundTrip(req)

	require.NoError(t, err)
	assert.Equal(t, "Bearer mine", sent.Header.Get("authorization"))
}

func TestAuthTransport_OnlyAuthenticatesRequestsToItsHost(t *testing.T) {
	for url, want := range map[string]string{
		"https://api.example.com/a":      "Bearer token-1",
		"https://API.example.com/a":      "Bearer token-1", // host names do not differ by case
		"https://api.example.com:8443/a": "Bearer token-1", // nor by port
		"https://other.example.com/a":    "",
		"https://sub.api.example.com/a":  "", // a subdomain is another host
		"https://api.example.com.evil/a": "",
	} {
		var sent *http.Request
		next := roundTripFunc(func(req *http.Request) (*http.Response, error) {
			sent = req
			return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
		})
		auth := &fakeAuth{}
		headers, _ := auth.lookup(nil, "")
		transport := newAuthTransport(next, "api.example.com", auth.lookup, headers, &options{now: time.Now, credentialsTTL: time.Minute})
		req, _ := http.NewRequest(http.MethodGet, url, nil)

		_, err := transport.RoundTrip(req)

		require.NoError(t, err, url)
		assert.Equal(t, want, sent.Header.Get("authorization"), url)
	}
}
