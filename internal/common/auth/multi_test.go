package auth

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/armadaproject/armada/internal/common/armadaerrors"
)

func TestNewMultiAuthService(t *testing.T) {
	principal := NewStaticPrincipal("test", "test", []string{"group"})
	failingService := &fakeAuthService{nil, errors.New("failed")}
	serviceWithoutCredentials := &fakeAuthService{nil, &armadaerrors.ErrMissingCredentials{}}
	successfulService := &fakeAuthService{principal, nil}

	sut := NewMultiAuthService([]AuthService{failingService, successfulService})
	_, e := sut.Authenticate(context.Background(), "")
	assert.NotNil(t, e, "failed auth should result in error")

	sut = NewMultiAuthService([]AuthService{serviceWithoutCredentials, successfulService})
	p, e := sut.Authenticate(context.Background(), "")
	assert.Nil(t, e)
	assert.Equal(t, principal.GetName(), p.GetName(), "principal should be returned")

	sut = NewMultiAuthService([]AuthService{serviceWithoutCredentials})
	p, e = sut.Authenticate(context.Background(), "")
	assert.NotNil(t, e, "no credentials should result in error")
}
