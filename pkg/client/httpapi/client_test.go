package httpapi

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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

// fakeAuth stands for the credentials: it counts the logins and the lookups of a token, and gives the headers
// authorization: Bearer token-N for the Nth lookup, so a request shows which lookup its token came from.
type fakeAuth struct {
	mu      sync.Mutex
	logins  int
	lookups int
	urls    []string
	// failFrom makes the lookups from the Nth on fail, if it is set.
	failFrom int
	loginErr error
}

func (f *fakeAuth) login() (func(context.Context, string) (map[string]string, error), error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.logins++
	if f.loginErr != nil {
		return nil, f.loginErr
	}
	return f.lookup, nil
}

func (f *fakeAuth) lookup(_ context.Context, url string) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lookups++
	f.urls = append(f.urls, url)
	if f.failFrom > 0 && f.lookups >= f.failFrom {
		return nil, assert.AnError
	}
	return map[string]string{"authorization": fmt.Sprintf("Bearer token-%d", f.lookups)}, nil
}

// recordingServer records the authorization header of each request it gets.
type recordingServer struct {
	*httptest.Server
	mu   sync.Mutex
	seen []string
}

func newRecordingServer(t *testing.T) *recordingServer {
	t.Helper()
	s := &recordingServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.seen = append(s.seen, r.Header.Get("authorization"))
		s.mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(s.Close)
	return s
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
	srv := newRecordingServer(t)
	c, err := New(&client.ApiConnectionDetails{BasicAuth: basic.LoginCredentials{Username: "u", Password: "p"}}, srv.URL)
	require.NoError(t, err)

	require.NoError(t, c.Get("/a", nil))
	require.NoError(t, c.Post("/b", map[string]any{}, nil))

	want := "basic " + base64.StdEncoding.EncodeToString([]byte("u:p"))
	assert.Equal(t, []string{want, want}, srv.seen)
}

func TestNew_SendsNothingWhenThereAreNoCredentials(t *testing.T) {
	srv := newRecordingServer(t)
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	require.NoError(t, c.Get("/a", nil))

	assert.Equal(t, []string{""}, srv.seen)
}

// Logging in can be interactive, so it happens once. What each request asks for is the current token, which the
// credentials renew when it has expired, so a command that runs longer than a token lasts keeps working.
func TestNew_LogsInOnceAndAsksForTheTokenOfEachRequest(t *testing.T) {
	srv := newRecordingServer(t)
	auth := &fakeAuth{}
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, auth.login, withCredentialsTTL(0))
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, c.Get("/a", nil))
	}

	assert.Equal(t, 1, auth.logins)
	assert.Equal(t, 5, auth.lookups, "one when the client was made, to check the url, and one for each request")
	assert.Equal(t, []string{"Bearer token-2", "Bearer token-3", "Bearer token-4", "Bearer token-5"}, srv.seen)
	assert.Equal(t, srv.URL, auth.urls[0], "the check is of the url of the api")
}

// Some credentials run a command to get a token, so the answer is kept for a short while.
func TestNew_KeepsTheTokenForAShortWhile(t *testing.T) {
	srv := newRecordingServer(t)
	auth := &fakeAuth{}
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, auth.login, withClock(func() time.Time { return now }))
	require.NoError(t, err)

	for i := 0; i < 4; i++ {
		require.NoError(t, c.Get("/a", nil))
	}
	assert.Equal(t, 1, auth.lookups)
	assert.Equal(t, []string{"Bearer token-1", "Bearer token-1", "Bearer token-1", "Bearer token-1"}, srv.seen)

	now = now.Add(credentialsTTL + time.Second)
	require.NoError(t, c.Get("/a", nil))
	assert.Equal(t, 2, auth.lookups)
	assert.Equal(t, "Bearer token-2", srv.seen[4], "asked again once the time is up")
}

func TestNew_AFailureToGetTheTokenFailsTheRequestAndSendsNothing(t *testing.T) {
	srv := newRecordingServer(t)
	auth := &fakeAuth{failFrom: 2}
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, auth.login, withCredentialsTTL(0))
	require.NoError(t, err)

	err = c.Get("/a", nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Empty(t, srv.seen)
}

func TestNew_ReportsAFailureToLogIn(t *testing.T) {
	auth := &fakeAuth{loginErr: assert.AnError}

	_, err := newClient(&client.ApiConnectionDetails{}, "localhost:8089", auth.login)

	assert.ErrorIs(t, err, assert.AnError)
}

func TestNew_ReportsAFailureToGetTheFirstToken(t *testing.T) {
	auth := &fakeAuth{failFrom: 1}

	_, err := newClient(&client.ApiConnectionDetails{}, "localhost:8089", auth.login)

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
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, (&fakeAuth{}).login, WithTransport(srv.Client().Transport))
	require.NoError(t, err)
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

	assert.Equal(t, "Bearer token-1", seen)
}

// The credentials are for the host of the API. A redirect to another host does not get them, even one that is local.
func TestDo_DoesNotSendTheCredentialsToAnotherHost(t *testing.T) {
	other := newRecordingServer(t)
	var seen string
	// The other server is the same machine but another host name, which is what makes it another host.
	otherUrl := "http://localhost:" + other.Listener.Addr().String()[len("127.0.0.1:"):] + "/api/thing"
	c := redirectingTo(t, otherUrl, &seen)

	require.NoError(t, c.Post("/api/thing", map[string]any{}, nil))

	assert.Equal(t, []string{""}, other.seen, "the request got there, and without the credentials")
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
