package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/armada/authorization/permissions"
)

func TestPrincipalPermissionChecker_UserHavePermission(t *testing.T) {

	checker := NewPrincipalPermissionChecker(map[permissions.Permission][]string{
		permissions.SubmitJobs: {"submitterGroup", "admin"},
	}, map[permissions.Permission][]string{})

	admin := NewStaticPrincipal("me", []string{"admin"})
	submitter := NewStaticPrincipal("me", []string{"submitterGroup"})
	otherUser := NewStaticPrincipal("me", []string{"test"})

	assert.True(t, checker.UserHasPermission(withPrincipal(context.Background(), admin), permissions.SubmitJobs))
	assert.True(t, checker.UserHasPermission(withPrincipal(context.Background(), submitter), permissions.SubmitJobs))
	assert.False(t, checker.UserHasPermission(withPrincipal(context.Background(), otherUser), permissions.SubmitJobs))
}
