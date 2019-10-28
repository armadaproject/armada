package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/metadata"

	"github.com/G-Research/armada/internal/armada/configuration"
	"github.com/G-Research/armada/internal/common"
)

func TestBasicAuthService(t *testing.T) {

	service := NewBasicAuthService(map[string]configuration.UserInfo{
		"root": {"toor", []string{}},
	})

	principal, e := service.Authenticate(metadata.NewIncomingContext(context.Background(), map[string][]string{
		common.UsernameField: {"root"},
		common.PasswordField: {"toor"},
	}))

	assert.Nil(t, e)
	assert.Equal(t, principal.GetName(), "root")

	_, e = service.Authenticate(metadata.NewIncomingContext(context.Background(), map[string][]string{
		common.UsernameField: {"root"},
		common.PasswordField: {"test"},
	}))

	assert.NotNil(t, e)
	assert.NotEqual(t, e, missingCredentials)

	_, e = service.Authenticate(context.Background())
	assert.Equal(t, e, missingCredentials)
}
