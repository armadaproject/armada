package authorization

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateMiddlewareAuthFunction(t *testing.T) {
	principal := NewStaticPrincipal("test", []string{"group"})
	failingService := &fakeAuthService{nil, errors.New("failed"), "alwaysFailService"}
	serviceWithoutCredentials := &fakeAuthService{nil, missingCredentialsErr, "missingCredsService"}
	successfulService := &fakeAuthService{principal, nil, "successService"}

	_, e := CreateMiddlewareAuthFunction([]AuthService{failingService, successfulService})(context.Background())
	assert.NotNil(t, e, "failed auth should result in error")

	c, e := CreateMiddlewareAuthFunction([]AuthService{serviceWithoutCredentials, successfulService})(context.Background())
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")

	_, e = CreateMiddlewareAuthFunction([]AuthService{serviceWithoutCredentials})(context.Background())
	assert.NotNil(t, e, "no credentials should result in error")
}

func TestCreateMiddlewareAuthFunctionContinuesEvenWithInvalidCreds(t *testing.T) {
	principal := NewStaticPrincipal("test", []string{"group"})
	invalidCredsService := &fakeAuthService{
		principal: principal,
		err:       invalidCredentialsErr,
		name:      "invalidCredsService",
	}
	secondInvalidCredsService := &fakeAuthService{
		principal: principal,
		err:       invalidCredentialsErr,
		name:      "secondInvalidCredsService",
	}
	missingCredsService := &fakeAuthService{
		principal: principal,
		err:       missingCredentialsErr,
		name:      "missingCredsService",
	}
	successfulService := &fakeAuthService{
		principal: principal,
		err:       nil,
		name:      "successService",
	}

	_, e := CreateMiddlewareAuthFunction(
		[]AuthService{
			invalidCredsService,
			missingCredsService,
			secondInvalidCredsService,
		})(context.Background())
	assert.NotNil(t, e, "failed auth should result in error")
	assert.Contains(t, e.Error(), invalidCredsService.Name())
	assert.Contains(t, e.Error(), missingCredsService.Name())
	assert.Contains(t, e.Error(), secondInvalidCredsService.Name())

	c, e := CreateMiddlewareAuthFunction(
		[]AuthService{
			invalidCredsService,
			missingCredsService,
			secondInvalidCredsService,
			successfulService,
		})(context.Background())
	assert.Nil(t, e)
	ctxPrincipal := GetPrincipal(c)
	assert.Equal(t, principal, ctxPrincipal, "principal should be added to context")
}

type fakeAuthService struct {
	principal Principal
	err       error
	name      string
}

func (f *fakeAuthService) Name() string {
	return f.name
}

func (f *fakeAuthService) Authenticate(ctx context.Context) (Principal, error) {
	return f.principal, f.err
}
