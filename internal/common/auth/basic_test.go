package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/armadaproject/armada/internal/common"
	"github.com/armadaproject/armada/internal/common/armadaerrors"
	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

func TestBasicAuthService(t *testing.T) {
	service := NewBasicAuthService(map[string]configuration.UserInfo{
		"root": {"toor", []string{}},
	})

	auth1 := basicPassword("root", "toor")
	principal, e := service.Authenticate(
		metadata.NewIncomingContext(context.Background(), auth1), auth1["authorization"][0])

	assert.Nil(t, e)
	assert.Equal(t, principal.GetName(), "root")

	auth2 := basicPassword("root", "test")
	_, e = service.Authenticate(
		metadata.NewIncomingContext(context.Background(), auth2), auth2["authorization"][0])

	assert.NotNil(t, e)
	var invalidCredsErr *armadaerrors.ErrInvalidCredentials
	assert.ErrorAs(t, e, &invalidCredsErr)

	_, e = service.Authenticate(context.Background(), "")
	var missingCredsErr *armadaerrors.ErrMissingCredentials
	assert.ErrorAs(t, e, &missingCredsErr)
}

func basicPassword(user, password string) map[string][]string {
	data, _ := (&common.LoginCredentials{
		Username: user,
		Password: password,
	}).GetRequestMetadata(context.Background())

	return map[string][]string{
		"authorization": {data["authorization"]},
	}
}
