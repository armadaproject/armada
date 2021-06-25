package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/auth/permission"
)

const TestSubmitPermission permission.Permission = "TestSubmitPermission"

func TestPrincipalPermissionChecker_UserHavePermission(t *testing.T) {

	checker := NewPrincipalPermissionChecker(
		map[permission.Permission][]string{TestSubmitPermission: {"submitterGroup", "admin"}},
		map[permission.Permission][]string{},
		map[permission.Permission][]string{})

	admin := NewStaticPrincipal("me", []string{"admin"})
	submitter := NewStaticPrincipal("me", []string{"submitterGroup"})
	otherUser := NewStaticPrincipal("me", []string{"test"})

	assert.True(t, checker.UserHasPermission(WithPrincipal(context.Background(), admin), TestSubmitPermission))
	assert.True(t, checker.UserHasPermission(WithPrincipal(context.Background(), submitter), TestSubmitPermission))
	assert.False(t, checker.UserHasPermission(WithPrincipal(context.Background(), otherUser), TestSubmitPermission))
}
