package authorization

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

	principal, e := service.Authenticate(
		metadata.NewIncomingContext(context.Background(), basicPassword("root", "toor")))

	assert.Nil(t, e)
	assert.Equal(t, principal.GetName(), "root")

	_, e = service.Authenticate(
		metadata.NewIncomingContext(context.Background(), basicPassword("root", "test")))

	assert.NotNil(t, e)
	var invalidCredsErr *armadaerrors.ErrInvalidCredentials
	assert.ErrorAs(t, e, &invalidCredsErr)

	_, e = service.Authenticate(context.Background())
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
