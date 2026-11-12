package auth

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

func TestCreateGrpcMiddlewareAuthFunction_Success(t *testing.T) {
	principal := NewStaticPrincipal("test", "test", []string{"group"})
	successfulService := &fakeAuthService{principal: principal}

	c, e := CreateGrpcMiddlewareAuthFunction(successfulService)(context.Background())
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")
}

func TestCreateGrpcMiddlewareAuthFunction_Failure(t *testing.T) {
	failingService := &fakeAuthService{err: errors.New("failed")}

	_, e := CreateGrpcMiddlewareAuthFunction(failingService)(context.Background())
	assert.NotNil(t, e, "failed auth should result in error")
}

func TestCreateGrpcMiddlewareAuthFunction_MissingCredentials(t *testing.T) {
	serviceWithoutCredentials := &fakeAuthService{err: &armadaerrors.ErrMissingCredentials{}}

	_, e := CreateGrpcMiddlewareAuthFunction(serviceWithoutCredentials)(context.Background())
	assert.NotNil(t, e, "no credentials should result in error")
}

func TestCreateHttpMiddlewareAuthFunction_Success(t *testing.T) {
	principal := NewStaticPrincipal("test", "test", []string{"group"})
	successfulService := &fakeAuthService{principal: principal}

	w := newFakeResponseWriter()
	var r http.Request
	c, e := CreateHttpMiddlewareAuthFunction(successfulService)(w, &r)
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")
}

func TestCreateHttpMiddlewareAuthFunction_Failure(t *testing.T) {
	failingService := &fakeAuthService{err: errors.New("failed")}

	w := newRecordingResponseWriter()
	var r http.Request
	_, e := CreateHttpMiddlewareAuthFunction(failingService)(w, &r)
	assert.NotNil(t, e, "failed auth should result in error")
	assert.Equal(t, http.StatusInternalServerError, w.status, "a failure that is not of the request is a server error")
	assert.Empty(t, w.Header().Get("WWW-Authenticate"))
}

func TestCreateHttpMiddlewareAuthFunction_MissingCredentials(t *testing.T) {
	serviceWithoutCredentials := &fakeAuthService{err: &armadaerrors.ErrMissingCredentials{}}

	w := newRecordingResponseWriter()
	var r http.Request
	_, e := CreateHttpMiddlewareAuthFunction(serviceWithoutCredentials)(w, &r)
	assert.NotNil(t, e, "no credentials should result in error")
	assert.Equal(t, http.StatusUnauthorized, w.status)
}

// A request that cannot be authenticated is the client's to fix, so it is a 401 that says how to authenticate, and
// not a server error. The body keeps the text it has always had.
func TestCreateHttpMiddlewareAuthFunction_Status(t *testing.T) {
	tests := map[string]struct {
		err    error
		status int
	}{
		"no scheme could authenticate it":   {&armadaerrors.ErrUnauthenticated{Message: "no scheme"}, http.StatusUnauthorized},
		"no credentials":                    {&armadaerrors.ErrMissingCredentials{}, http.StatusUnauthorized},
		"credentials that are not valid":    {&armadaerrors.ErrInvalidCredentials{Username: "u", AuthService: "Basic"}, http.StatusUnauthorized},
		"wrapped, as errors can be":         {fmt.Errorf("verifying: %w", &armadaerrors.ErrInvalidCredentials{}), http.StatusUnauthorized},
		"the auth service failing":          {&armadaerrors.ErrInternalAuthServiceError{Message: "oidc provider unreachable"}, http.StatusInternalServerError},
		"an error that is not of the kinds": {errors.New("failed"), http.StatusInternalServerError},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			w := newRecordingResponseWriter()
			var r http.Request

			_, e := CreateHttpMiddlewareAuthFunction(&fakeAuthService{nil, tc.err, []string{"Bearer"}})(w, &r)

			assert.Equal(t, tc.err, e, "the error is still returned")
			assert.Equal(t, tc.status, w.status)
			assert.Contains(t, w.body.String(), "auth error:", "the body is as it was")
			assert.Contains(t, w.body.String(), tc.err.Error())
			if tc.status == http.StatusUnauthorized {
				assert.Equal(t, []string{"Bearer"}, w.Header().Values("WWW-Authenticate"), "what the service accepts")
			} else {
				assert.Empty(t, w.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

// Through the services as they are put together, a request nobody can authenticate is a 401.
func TestCreateHttpMiddlewareAuthFunction_NoServiceCanAuthenticateIt(t *testing.T) {
	multi := NewMultiAuthService([]AuthService{&fakeAuthService{err: &armadaerrors.ErrMissingCredentials{}}})
	w := newRecordingResponseWriter()
	var r http.Request

	_, e := CreateHttpMiddlewareAuthFunction(multi)(w, &r)

	require.Error(t, e)
	assert.Equal(t, http.StatusUnauthorized, w.status)
	assert.Contains(t, w.body.String(), "could not be authenticated with any of the supported schemes")
}

// The challenge is built from the services that are enabled, so a client is told only of schemes that can work.
func TestCreateHttpMiddlewareAuthFunction_ChallengesAreThoseOfTheEnabledServices(t *testing.T) {
	kubernetes := NewKubernetesNativeAuthService(configuration.KubernetesAuthConfig{KidMappingFileLocation: "/kids/"})
	tests := map[string]struct {
		services   []AuthService
		challenges []string
	}{
		"kubernetes only": {
			[]AuthService{&kubernetes},
			[]string{"KubernetesAuth"},
		},
		"oidc only": {
			[]AuthService{&OpenIdAuthService{}},
			[]string{"Bearer"},
		},
		"basic and oidc": {
			[]AuthService{NewBasicAuthService(nil), &OpenIdAuthService{}},
			[]string{`Basic realm="armada"`, "Bearer"},
		},
		"a service with no scheme adds none": {
			[]AuthService{&fakeAuthService{err: &armadaerrors.ErrMissingCredentials{}}, &OpenIdAuthService{}},
			[]string{"Bearer"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			w := newRecordingResponseWriter()
			unauthenticated := &fakeAuthService{err: &armadaerrors.ErrMissingCredentials{}}
			services := append([]AuthService{unauthenticated}, tc.services...)
			var r http.Request

			_, e := CreateHttpMiddlewareAuthFunction(NewMultiAuthService(services))(w, &r)

			require.Error(t, e)
			assert.Equal(t, tc.challenges, w.Header().Values("WWW-Authenticate"))
		})
	}
}

func TestHttpStatusForAuthError_IsUnauthorizedForTheSameErrorsAsGrpc(t *testing.T) {
	for _, err := range []error{
		&armadaerrors.ErrUnauthenticated{},
		&armadaerrors.ErrMissingCredentials{},
		&armadaerrors.ErrInvalidCredentials{},
	} {
		assert.Equal(t, codes.Unauthenticated, status.Code(err.(interface{ GRPCStatus() *status.Status }).GRPCStatus().Err()), "%T", err)
		assert.Equal(t, http.StatusUnauthorized, httpStatusForAuthError(err), "%T", err)
	}
}

type fakeAuthService struct {
	principal  Principal
	err        error
	challenges []string
}

func (f *fakeAuthService) Challenges() []string {
	return f.challenges
}

func (f *fakeAuthService) Authenticate(ctx context.Context, authHeader string) (Principal, error) {
	return f.principal, f.err
}

type fakeResponseWriter struct {
	header map[string][]string
}

func newFakeResponseWriter() http.ResponseWriter {
	return fakeResponseWriter{
		header: map[string][]string{},
	}
}

func (w fakeResponseWriter) Header() http.Header {
	return w.header
}

func (w fakeResponseWriter) Write([]byte) (int, error) {
	return 0, nil
}

func (w fakeResponseWriter) WriteHeader(statusCode int) {
}

// recordingResponseWriter records the status and the body that are written.
type recordingResponseWriter struct {
	header http.Header
	status int
	body   *bytes.Buffer
}

func newRecordingResponseWriter() *recordingResponseWriter {
	return &recordingResponseWriter{header: http.Header{}, body: &bytes.Buffer{}}
}

func (w *recordingResponseWriter) Header() http.Header         { return w.header }
func (w *recordingResponseWriter) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *recordingResponseWriter) WriteHeader(statusCode int)  { w.status = statusCode }
