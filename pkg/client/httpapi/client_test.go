package httpapi

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client"
	"github.com/armadaproject/armada/pkg/client/auth/basic"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return srv
}

// withToken is a way to resolve the credentials that gives every request the header authorization: token.
func withToken(resolved *int, resolvedFor *string) func(context.Context, string) (map[string]string, error) {
	return func(_ context.Context, url string) (map[string]string, error) {
		*resolved++
		*resolvedFor = url
		return map[string]string{"authorization": "token"}, nil
	}
}

func TestPost_SendsTheBodyAsJsonAndDecodesTheResponse(t *testing.T) {
	var method, path, contentType, body string
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		method, path, contentType, body = r.Method, r.URL.Path, r.Header.Get("Content-Type"), string(b)
		_, _ = w.Write([]byte(`{"answer": 42}`))
	})
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	var out struct{ Answer int }
	err = c.Post("/api/thing", map[string]any{"question": "?"}, &out)

	require.NoError(t, err)
	assert.Equal(t, http.MethodPost, method)
	assert.Equal(t, "/api/thing", path)
	assert.Equal(t, "application/json", contentType)
	assert.JSONEq(t, `{"question": "?"}`, body)
	assert.Equal(t, 42, out.Answer)
}

func TestGet_SendsNoBody(t *testing.T) {
	var method, contentType, body string
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		method, contentType, body = r.Method, r.Header.Get("Content-Type"), string(b)
		_, _ = w.Write([]byte(`{"answer": 42}`))
	})
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	var out struct{ Answer int }
	require.NoError(t, c.Get("/api/thing", &out))

	assert.Equal(t, http.MethodGet, method)
	assert.Empty(t, contentType)
	assert.Empty(t, body)
	assert.Equal(t, 42, out.Answer)
}

func TestDo_IgnoresTheResponseWhenThereIsNowhereToDecodeItTo(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	assert.NoError(t, c.Do(http.MethodDelete, "/api/thing", nil, nil))
}

func TestDo_ReportsTheStatusAndTheStartOfTheResponseOfAFailedRequest(t *testing.T) {
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("auth error: bad credentials"))
	})
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	err = c.Post("/api/thing", map[string]any{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "POST /api/thing returned 500")
	assert.Contains(t, err.Error(), "auth error: bad credentials")
	// Callers that know what a status means can find it.
	var status *StatusError
	require.ErrorAs(t, err, &status)
	assert.Equal(t, http.StatusInternalServerError, status.Code)
	assert.Equal(t, "auth error: bad credentials", status.Body)
	assert.Equal(t, http.MethodPost, status.Method)
	assert.Equal(t, "/api/thing", status.Path)
}

func TestNew_NeedsAUrl(t *testing.T) {
	_, err := New(&client.ApiConnectionDetails{}, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no url given")
}

func TestNew_SendsTheCredentialsOfTheConnectionOnEveryRequest(t *testing.T) {
	var seen []string
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("authorization"))
		_, _ = w.Write([]byte(`{}`))
	})
	c, err := New(&client.ApiConnectionDetails{BasicAuth: basic.LoginCredentials{Username: "u", Password: "p"}}, srv.URL)
	require.NoError(t, err)

	require.NoError(t, c.Get("/a", nil))
	require.NoError(t, c.Post("/b", map[string]any{}, nil))

	want := "basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	assert.Equal(t, []string{want, want}, seen)
}

// Resolving the credentials can log in, which may be interactive, so it happens once for all the requests.
func TestNew_ResolvesTheCredentialsOnce(t *testing.T) {
	var seen []string
	srv := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		seen = append(seen, r.Header.Get("authorization"))
		_, _ = w.Write([]byte(`{}`))
	})
	resolved, resolvedFor := 0, ""
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, withToken(&resolved, &resolvedFor))
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, c.Get("/a", nil))
	}

	assert.Equal(t, 1, resolved)
	assert.Equal(t, srv.URL, resolvedFor)
	assert.Equal(t, []string{"token", "token", "token", "token"}, seen)
}

func TestNew_ReportsAFailureToResolveTheCredentials(t *testing.T) {
	_, err := newClient(&client.ApiConnectionDetails{}, "localhost:8089", func(context.Context, string) (map[string]string, error) {
		return nil, assert.AnError
	})

	assert.ErrorIs(t, err, assert.AnError)
}

func TestNew_RefusesToSendCredentialsOverPlainHttpToARemoteHost(t *testing.T) {
	credentials := basic.LoginCredentials{Username: "u", Password: "p"}

	_, err := New(&client.ApiConnectionDetails{BasicAuth: credentials}, "http://api.example.com")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to send credentials over plain http")

	// There is nothing to protect without credentials, and a connection that forces no tls has chosen this.
	_, err = New(&client.ApiConnectionDetails{}, "http://api.example.com")
	assert.NoError(t, err)
	_, err = New(&client.ApiConnectionDetails{BasicAuth: credentials, ForceNoTls: true}, "http://api.example.com")
	assert.NoError(t, err)
}

// redirectingTo is a client for a TLS server that redirects every request to target, except those to /moved.
// The credentials the server sees on /moved are recorded in seen.
func redirectingTo(t *testing.T, target string, seen *string) *Client {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/moved" {
			*seen = r.Header.Get("authorization")
			_, _ = w.Write([]byte(`{}`))
			return
		}
		http.Redirect(w, r, target, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(srv.Close)
	resolved, resolvedFor := 0, ""
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, withToken(&resolved, &resolvedFor))
	require.NoError(t, err)
	c.httpClient.Transport = srv.Client().Transport // trusts the certificate of the test server
	return c
}

// The credentials are checked against the url a request starts at, and a redirect to the same host keeps
// them, so a redirect must not be able to move the request from https to plain http.
func TestDo_RefusesToFollowARedirectToPlainHttpWithCredentials(t *testing.T) {
	var seen string
	c := redirectingTo(t, "http://api.example.com/moved", &seen)

	err := c.Post("/api/thing", map[string]any{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "refusing to follow a redirect to http://api.example.com/moved")
	assert.Empty(t, seen)
}

func TestDo_FollowsARedirectOverHttpsWithCredentials(t *testing.T) {
	var seen string
	c := redirectingTo(t, "/moved", &seen)

	require.NoError(t, c.Post("/api/thing", map[string]any{}, nil))

	assert.Equal(t, "token", seen)
}

func TestDo_FollowsARedirectToALocalServerWithCredentials(t *testing.T) {
	var reached bool
	local := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		reached = true
		_, _ = w.Write([]byte(`{}`))
	})
	var seen string
	c := redirectingTo(t, local.URL+"/api/thing", &seen)

	require.NoError(t, c.Post("/api/thing", map[string]any{}, nil))

	assert.True(t, reached)
}

func TestDo_StopsAfterTooManyRedirects(t *testing.T) {
	var seen string
	c := redirectingTo(t, "/api/thing", &seen)

	err := c.Post("/api/thing", map[string]any{}, nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "stopped after 10 redirects")
}

func TestBaseUrl(t *testing.T) {
	for in, want := range map[string]string{
		"localhost:8089":                  "http://localhost:8089",
		"127.0.0.1:8089":                  "http://127.0.0.1:8089",
		"api.example.com":                 "https://api.example.com",
		"localhost.evil.com":              "https://localhost.evil.com",
		"http://api.example.com":          "http://api.example.com",
		"https://api.example.com/":        "https://api.example.com",
		"https://api.example.com///":      "https://api.example.com",
		"https://api.example.com/prefix/": "https://api.example.com/prefix",
	} {
		assert.Equal(t, want, baseUrl(in), in)
	}
}
