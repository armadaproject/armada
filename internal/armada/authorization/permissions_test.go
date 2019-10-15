package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrincipalPermissionChecker_UserHavePermission(t *testing.T) {

	checker := NewPrincipalPermissionChecker(map[Permission][]string{
		SubmitJobs: {"submitterGroup", "admin"},
	})

	admin := NewStaticPrincipal("me", []string{"admin"})
	submitter := NewStaticPrincipal("me", []string{"submitterGroup"})
	otherUser := NewStaticPrincipal("me", []string{"test"})

	assert.True(t, checker.UserHavePermission(withPrincipal(context.Background(), admin), SubmitJobs))
	assert.True(t, checker.UserHavePermission(withPrincipal(context.Background(), submitter), SubmitJobs))
	assert.False(t, checker.UserHavePermission(withPrincipal(context.Background(), otherUser), SubmitJobs))
}
