package auth

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func TestCreateGrpcMiddlewareAuthFunction_Success(t *testing.T) {
	principal := NewStaticPrincipal("test", "test", []string{"group"})
	successfulService := &fakeAuthService{principal, nil}

	c, e := CreateGrpcMiddlewareAuthFunction(successfulService)(context.Background())
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")
}

func TestCreateGrpcMiddlewareAuthFunction_Failure(t *testing.T) {
	failingService := &fakeAuthService{nil, errors.New("failed")}

	_, e := CreateGrpcMiddlewareAuthFunction(failingService)(context.Background())
	assert.NotNil(t, e, "failed auth should result in error")
}

func TestCreateGrpcMiddlewareAuthFunction_MissingCredentials(t *testing.T) {
	serviceWithoutCredentials := &fakeAuthService{nil, &armadaerrors.ErrMissingCredentials{}}

	_, e := CreateGrpcMiddlewareAuthFunction(serviceWithoutCredentials)(context.Background())
	assert.NotNil(t, e, "no credentials should result in error")
}

func TestCreateHttpMiddlewareAuthFunction_Success(t *testing.T) {
	principal := NewStaticPrincipal("test", "test", []string{"group"})
	successfulService := &fakeAuthService{principal, nil}

	w := newFakeResponseWriter()
	var r http.Request
	c, e := CreateHttpMiddlewareAuthFunction(successfulService)(w, &r)
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")
}

func TestCreateHttpMiddlewareAuthFunction_Failure(t *testing.T) {
	failingService := &fakeAuthService{nil, errors.New("failed")}

	w := newFakeResponseWriter()
	var r http.Request
	_, e := CreateHttpMiddlewareAuthFunction(failingService)(w, &r)
	assert.NotNil(t, e, "failed auth should result in error")
}

func TestCreateHttpMiddlewareAuthFunction_MissingCredentials(t *testing.T) {
	serviceWithoutCredentials := &fakeAuthService{nil, &armadaerrors.ErrMissingCredentials{}}

	w := newFakeResponseWriter()
	var r http.Request
	_, e := CreateHttpMiddlewareAuthFunction(serviceWithoutCredentials)(w, &r)
	assert.NotNil(t, e, "no credentials should result in error")
}

type fakeAuthService struct {
	principal Principal
	err       error
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
