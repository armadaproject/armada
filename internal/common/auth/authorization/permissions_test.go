package authorization

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/G-Research/armada/internal/common/auth/permission"
)

const submitJobsPermission permission.Permission = "submit_jobs"
const createQueuePermission permission.Permission = "create_queue"
const executeJobsPermission permission.Permission = "execute_jobs"
const watchEventsPermission permission.Permission = "watch_events"

const submitterGroup = "submitterGroup"
const adminGroup = "adminGroup"
const unimportantGroup = "unimportantGroup"
const creatorScope = "creatorScope"
const executorClaim = "executorClaim"
const thingOwningGroup = "thingOwningGroup"

var ctx = context.Background()

var checker *PrincipalPermissionChecker
var admin Principal
var submitter Principal
var otherUser Principal
var userWithCreatorScope Principal
var userWithExecutorClaim Principal
var thingOwnerDirect Principal
var thingOwnerDirectAndViaGroup Principal
var thingNonOwner Principal
var thingOwnerViaGroup Principal

type OwnedThing struct {
	UserOwners  []string
	GroupOwners []string
}

func (ot *OwnedThing) GetUserOwners() []string {
	if ot != nil {
		return ot.UserOwners
	}
	return nil
}

func (ot *OwnedThing) GetGroupOwners() []string {
	if ot != nil {
		return ot.GroupOwners
	}
	return nil
}

var ownedThing Owned

func init() {
	checker = NewPrincipalPermissionChecker(
		map[permission.Permission][]string{submitJobsPermission: {submitterGroup, adminGroup}, watchEventsPermission: {EveryoneGroup}},
		map[permission.Permission][]string{createQueuePermission: {creatorScope}},
		map[permission.Permission][]string{executeJobsPermission: {executorClaim}},
	)

	admin = NewStaticPrincipal("admin", []string{adminGroup})
	submitter = NewStaticPrincipal("submitter", []string{submitterGroup})
	otherUser = NewStaticPrincipal("otherUser", []string{unimportantGroup})
	userWithCreatorScope = NewStaticPrincipalWithScopesAndClaims("creatorScopeUser", []string{unimportantGroup}, []string{creatorScope}, []string{})
	userWithExecutorClaim = NewStaticPrincipalWithScopesAndClaims("executorClaimUser", []string{unimportantGroup}, []string{}, []string{executorClaim})

	thingOwnerDirect = NewStaticPrincipal("thingOwnerDirect", []string{})
	thingOwnerDirectAndViaGroup = NewStaticPrincipal("thingOwnerDirectAndViaGroup", []string{thingOwningGroup})
	thingOwnerViaGroup = NewStaticPrincipal("thingOwnerViaGroup", []string{thingOwningGroup})
	thingNonOwner = NewStaticPrincipal("thingNonOwner", []string{unimportantGroup})
	ownedThing = &OwnedThing{
		[]string{thingOwnerDirect.GetName(), thingOwnerDirectAndViaGroup.GetName()},
		[]string{thingOwningGroup},
	}
}

func TestPrincipalPermissionChecker_EveryoneCanDoEveryoneThings(t *testing.T) {
	for _, u := range []Principal{admin, submitter, otherUser, userWithCreatorScope, userWithExecutorClaim} {
		assert.True(t, checker.UserHasPermission(WithPrincipal(ctx, u), watchEventsPermission))
	}
}

func TestPrincipalPermissionChecker_UserJustInGroupWithPermissionCan(t *testing.T) {
	assert.True(t, checker.UserHasPermission(WithPrincipal(ctx, admin), submitJobsPermission))
}
func TestPrincipalPermissionChecker_OtherUserJustInGroupWithPermissionCan(t *testing.T) {
	assert.True(t, checker.UserHasPermission(WithPrincipal(ctx, submitter), submitJobsPermission))
}

func TestPrincipalPermissionChecker_UserJustInGroupWithoutPermissionCant(t *testing.T) {
	assert.False(t, checker.UserHasPermission(WithPrincipal(ctx, otherUser), submitJobsPermission))
}

func TestPrincipalPermissionChecker_UserWithScopeCan(t *testing.T) {
	assert.True(t, checker.UserHasPermission(WithPrincipal(ctx, userWithCreatorScope), createQueuePermission))
}

func TestPrincipalPermissionChecker_UsersWithoutScopeCant(t *testing.T) {
	for _, u := range []Principal{admin, submitter, otherUser, userWithExecutorClaim} {
		assert.False(t, checker.UserHasPermission(WithPrincipal(ctx, u), createQueuePermission))
	}
}

func TestPrincipalPermissionChecker_UserWithClaimCan(t *testing.T) {
	assert.True(t, checker.UserHasPermission(WithPrincipal(ctx, userWithExecutorClaim), executeJobsPermission))
}

func TestPrincipalPermissionChecker_UsersWithoutClaimsCant(t *testing.T) {
	for _, u := range []Principal{admin, submitter, otherUser, userWithCreatorScope} {
		assert.False(t, checker.UserHasPermission(WithPrincipal(ctx, u), executeJobsPermission))
	}
}

func TestUserOwns_confirmsOwnershipOfGrouplessOwner(t *testing.T) {
	is_owner, groups_granting_ownership := checker.UserOwns(WithPrincipal(ctx, thingOwnerDirect), ownedThing)
	assert.True(t, is_owner)
	assert.Empty(t, groups_granting_ownership)
}

func TestUserOwns_confirmsOwnershipOfDirectOwnerInOwningGroup(t *testing.T) {
	is_owner, groups_granting_ownership := checker.UserOwns(WithPrincipal(ctx, thingOwnerDirectAndViaGroup), ownedThing)
	assert.True(t, is_owner)
	// A comment above the implementation at the time I wrote this test says
	// these groups should NOT return groups through which the user owns the
	// the resource if the user is also a direct owner. If this changes, the
	// assertion below will need to be rewritten.
	assert.Empty(t, groups_granting_ownership)
}

func TestUserOwns_confirmsOwnershipOfThingOwnerViaGroup(t *testing.T) {
	is_owner, groups_granting_ownership := checker.UserOwns(WithPrincipal(ctx, thingOwnerViaGroup), ownedThing)
	assert.True(t, is_owner)
	assert.EqualValues(t, groups_granting_ownership, []string{thingOwningGroup})
}

func TestUserOwns_refutesOwnershipOfThingNonOwner(t *testing.T) {
	is_owner, groups_granting_ownership := checker.UserOwns(WithPrincipal(ctx, thingNonOwner), ownedThing)
	assert.False(t, is_owner)
	assert.Empty(t, groups_granting_ownership)
}
