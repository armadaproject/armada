package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func TestCreateMiddlewareAuthFunction(t *testing.T) {
	principal := NewStaticPrincipal("test", []string{"group"})
	failingService := &fakeAuthService{nil, errors.New("failed")}
	serviceWithoutCredentials := &fakeAuthService{nil, &armadaerrors.ErrMissingCredentials{}}
	successfulService := &fakeAuthService{principal, nil}

	_, e := CreateMiddlewareAuthFunction([]AuthService{failingService, successfulService})(context.Background())
	assert.NotNil(t, e, "failed auth should result in error")

	c, e := CreateMiddlewareAuthFunction([]AuthService{serviceWithoutCredentials, successfulService})(context.Background())
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")

	_, e = CreateMiddlewareAuthFunction([]AuthService{serviceWithoutCredentials})(context.Background())
	assert.NotNil(t, e, "no credentials should result in error")
}

type fakeAuthService struct {
	principal Principal
	err       error
}

func (f *fakeAuthService) Name() string {
	return "Fake"
}

func (f *fakeAuthService) Authenticate(ctx context.Context) (Principal, error) {
	return f.principal, f.err
}
