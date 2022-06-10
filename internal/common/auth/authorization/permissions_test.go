package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/auth/permission"
)

const TestSubmitJobsPermission permission.Permission = "submit_jobs"

func TestPrincipalPermissionChecker_UserHavePermission(t *testing.T) {

	checker := NewPrincipalPermissionChecker(
		map[permission.Permission][]string{TestSubmitJobsPermission: {"submitterGroup", "adminGroup"}},
		map[permission.Permission][]string{},
		map[permission.Permission][]string{})

	admin := NewStaticPrincipal("admin", []string{"adminGroup"})
	submitter := NewStaticPrincipal("submitter", []string{"submitterGroup"})
	otherUser := NewStaticPrincipal("otherUser", []string{"someUnimportantGroup"})

	assert.True(t, checker.UserHasPermission(WithPrincipal(context.Background(), admin), TestSubmitJobsPermission))
	assert.True(t, checker.UserHasPermission(WithPrincipal(context.Background(), submitter), TestSubmitJobsPermission))
	assert.False(t, checker.UserHasPermission(WithPrincipal(context.Background(), otherUser), TestSubmitJobsPermission))
}
