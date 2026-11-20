package httpapi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/pkg/client"
)

// authServer answers 401 to the tokens it does not accept, and 200 to those it does. It records what each request
// carried: the token, and the body.
type authServer struct {
	*httptest.Server
	mu       sync.Mutex
	accepted map[string]bool
	status   int // the status of a request it does not accept: 401 unless set
	tokens   []string
	bodies   []string
}

func newAuthServer(t *testing.T, accepted ...string) *authServer {
	t.Helper()
	s := &authServer{accepted: map[string]bool{}}
	for _, a := range accepted {
		s.accepted["Bearer "+a] = true
	}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s.mu.Lock()
		s.tokens = append(s.tokens, r.Header.Get("authorization"))
		s.bodies = append(s.bodies, string(b))
		ok := s.accepted[r.Header.Get("authorization")]
		s.mu.Unlock()
		if !ok {
			code := s.status
			if code == 0 {
				code = http.StatusUnauthorized
			}
			w.WriteHeader(code)
			_, _ = w.Write([]byte("auth error: rejected"))
			return
		}
		_, _ = w.Write([]byte(`{"ok": true}`))
	}))
	t.Cleanup(s.Close)
	return s
}

// tokenSequence stands for credentials that give a new token each time they are asked: token-1, token-2, and so on.
func tokenSequence(start int, asked *int) func() (func(context.Context, string) (map[string]string, error), error) {
	return func() (func(context.Context, string) (map[string]string, error), error) {
		n := start - 1
		return func(context.Context, string) (map[string]string, error) {
			n++
			*asked++
			return map[string]string{"authorization": fmt.Sprintf("Bearer token-%d", n)}, nil
		}, nil
	}
}

// A token that was kept and has since been rejected, because it expired or was revoked, is replaced and the request is
// sent again.
func TestUnauthorized_AKeptTokenThatIsRejectedIsReplacedAndTheRequestSentAgain(t *testing.T) {
	srv := newAuthServer(t, "token-2")
	asked := 0
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, tokenSequence(1, &asked)) // token-1 is asked for first
	require.NoError(t, err)

	var out struct{ OK bool }
	err = c.Post("/api/thing", map[string]any{"q": "a question"}, &out)

	require.NoError(t, err)
	assert.True(t, out.OK)
	assert.Equal(t, []string{"Bearer token-1", "Bearer token-2"}, srv.tokens, "rejected once, then sent with a new token")
	assert.Equal(t, []string{`{"q":"a question"}`, `{"q":"a question"}`}, srv.bodies, "with the same body both times")
	assert.Equal(t, 2, asked)
}

// Credentials that were asked for for this very request would give the same answer again.
func TestUnauthorized_ATokenThatWasJustAskedForIsNotAskedForAgain(t *testing.T) {
	srv := newAuthServer(t) // accepts nothing
	asked := 0
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, tokenSequence(1, &asked), withCredentialsTTL(0))
	require.NoError(t, err)
	asked = 0 // the one when the client was made

	err = c.Get("/api/thing", nil)

	var status *StatusError
	require.ErrorAs(t, err, &status)
	assert.True(t, status.Unauthorized())
	assert.Equal(t, []string{"Bearer token-2"}, srv.tokens, "sent once: the token was fresh")
	assert.Equal(t, 1, asked)
}

func TestUnauthorized_ARetryThatIsRejectedToIsReported(t *testing.T) {
	srv := newAuthServer(t) // accepts nothing
	asked := 0
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, tokenSequence(1, &asked))
	require.NoError(t, err)

	err = c.Get("/api/thing", nil)

	var status *StatusError
	require.ErrorAs(t, err, &status)
	assert.Equal(t, http.StatusUnauthorized, status.Code)
	assert.Equal(t, []string{"Bearer token-1", "Bearer token-2"}, srv.tokens, "sent twice, and no more")
	assert.Contains(t, err.Error(), "auth error: rejected", "what the server said")
	assert.Contains(t, err.Error(), "were not accepted, even when asked for again")
}

// Credentials that give the same headers again cannot be expected to be accepted, so the request is not sent again.
func TestUnauthorized_TheSameCredentialsAreNotTriedTwice(t *testing.T) {
	srv := newAuthServer(t)
	login := func() (func(context.Context, string) (map[string]string, error), error) {
		return func(context.Context, string) (map[string]string, error) {
			return map[string]string{"authorization": "Bearer same"}, nil
		}, nil
	}
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, login)
	require.NoError(t, err)

	err = c.Get("/api/thing", nil)

	require.Error(t, err)
	assert.Equal(t, []string{"Bearer same"}, srv.tokens)
}

// Only a 401 says the token is the problem.
func TestUnauthorized_OtherStatusesAreNotRetried(t *testing.T) {
	for _, code := range []int{http.StatusForbidden, http.StatusInternalServerError, http.StatusBadRequest, http.StatusServiceUnavailable} {
		srv := newAuthServer(t)
		srv.status = code
		asked := 0
		c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, tokenSequence(1, &asked))
		require.NoError(t, err)

		err = c.Get("/api/thing", nil)

		var status *StatusError
		require.ErrorAs(t, err, &status, code)
		assert.Equal(t, code, status.Code)
		assert.Len(t, srv.tokens, 1, "%d is not retried", code)
		assert.Empty(t, status.Hint, "%d has no hint", code)
	}
}

func TestUnauthorized_AnAcceptedTokenIsNotAskedForAgainOrRetried(t *testing.T) {
	srv := newAuthServer(t, "token-1")
	asked := 0
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, tokenSequence(1, &asked))
	require.NoError(t, err)

	require.NoError(t, c.Get("/a", nil))
	require.NoError(t, c.Get("/b", nil))

	assert.Equal(t, []string{"Bearer token-1", "Bearer token-1"}, srv.tokens)
	assert.Equal(t, 1, asked)
}

// A request whose body cannot be read again cannot be sent again.
func TestUnauthorized_ARequestWithABodyThatCannotBeReadAgainIsNotRetried(t *testing.T) {
	srv := newAuthServer(t, "token-2")
	asked := 0
	login := tokenSequence(1, &asked)
	auth, _ := login()
	headers, _ := auth(context.Background(), "")
	transport := newAuthTransport(http.DefaultTransport, hostOf(srv.URL), auth, headers, &options{now: time.Now, credentialsTTL: time.Hour})
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/a", io.NopCloser(strings.NewReader("body")))
	require.NoError(t, err)
	require.Nil(t, req.GetBody, "a body of an unknown kind has no way to be read again")

	resp, err := transport.RoundTrip(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, []string{"Bearer token-1"}, srv.tokens, "not sent again")
}

func TestUnauthorized_AFailureToAskForNewCredentialsIsAnErrorThatSaysSo(t *testing.T) {
	srv := newAuthServer(t)
	auth := &fakeAuth{failFrom: 2}
	c, err := newClient(&client.ApiConnectionDetails{}, srv.URL, auth.login)
	require.NoError(t, err)

	err = c.Get("/a", nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
	assert.Contains(t, err.Error(), "asking for new credentials after the request was not authorized")
}

func TestUnauthorized_WithNoCredentialsTheHintSaysSo(t *testing.T) {
	srv := newAuthServer(t)
	c, err := New(&client.ApiConnectionDetails{}, srv.URL)
	require.NoError(t, err)

	err = c.Get("/a", nil)

	var status *StatusError
	require.ErrorAs(t, err, &status)
	assert.Contains(t, status.Hint, "no credentials")
	assert.Len(t, srv.tokens, 1, "there is nothing to ask for")
	assert.Contains(t, err.Error(), "the connection has no credentials")
}

// A request to a host the credentials are not for is neither given them nor sent again.
func TestUnauthorized_ARequestToAnotherHostIsLeftAlone(t *testing.T) {
	other := newAuthServer(t)
	asked := 0
	auth, _ := tokenSequence(1, &asked)()
	headers, _ := auth(context.Background(), "")
	transport := newAuthTransport(http.DefaultTransport, "api.example.com", auth, headers, &options{now: time.Now, credentialsTTL: time.Hour})
	req, _ := http.NewRequest(http.MethodGet, other.URL+"/a", nil)

	resp, err := transport.RoundTrip(req)

	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Equal(t, []string{""}, other.tokens, "no credentials")
	assert.Equal(t, 1, asked, "and none asked for")
}
