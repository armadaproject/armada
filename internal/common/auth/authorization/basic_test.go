package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/G-Research/armada/internal/common"
	"github.com/G-Research/armada/internal/common/auth/configuration"
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
	assert.NotEqual(t, e, missingCredentials)

	_, e = service.Authenticate(context.Background())
	assert.Equal(t, e, missingCredentials)
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
