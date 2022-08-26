package queue

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestNewPermissionsFromOwners(t *testing.T) {
	testCase := func(users, groups []string) bool {
		permissions := NewPermissionsFromOwners(users, groups)

		for index, user := range users {
			subject := permissions.Subjects[index]
			if subject.Kind != PermissionSubjectKindUser {
				t.Errorf("[index: %d] invalid subject kind: %s. Expected: %s", index, subject.Kind, PermissionSubjectKindUser)
				return false
			}
			if subject.Name != user {
				t.Errorf("[index: %d] invalid subject name: %s. Expected: %s", index, subject.Name, user)
				return false
			}
		}

		for index, group := range groups {
			index += len(users)
			subject := permissions.Subjects[index]
			if subject.Kind != PermissionSubjectKindGroup {
				t.Errorf("[index: %d] invalid subject kind: %s. Expected: %s", index, subject.Kind, PermissionSubjectKindUser)
				return false
			}
			if subject.Name != group {
				t.Errorf("[index: %d] invalid subject name: %s. Expected: %s", index, subject.Name, group)
				return false
			}
		}

		if !reflect.DeepEqual(permissions.Verbs, AllPermissionVerbs()) {
			t.Errorf("Invalid permission verbs: %v. Expected: %v", permissions.Verbs, AllPermissionVerbs())
			return false
		}

		return true
	}

	if err := quick.Check(testCase, nil); err != nil {
		t.Fatal(err)
	}
}

func TestPermissionsToAPI(t *testing.T) {
	testCase := func(permissions1 Permissions) bool {
		permissions2, err := NewPermissions(permissions1.ToAPI())
		if err != nil {
			t.Errorf("failed to generate permissions from api.Queue_Permissions: %s", err)
			return false
		}

		return reflect.DeepEqual(permissions1, permissions2)
	}

	if err := quick.Check(testCase, nil); err != nil {
		t.Fatal(err)
	}
}
