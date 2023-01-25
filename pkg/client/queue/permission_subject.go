package queue

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

type PermissionSubject struct {
	Kind PermissionSubjectKind `json:"kind"`
	Name string                `json:"name"`
}

type PermissionSubjects []PermissionSubject

// NewPermissionSibjects returns PermissionSubjects using the subjects values. If any of the subjects
// has an invalid subject kind an error is returned.
func NewPermissionSubjects(subjects []*api.Queue_Permissions_Subject) (PermissionSubjects, error) {
	result := PermissionSubjects{}

	for index, subject := range subjects {
		if subject == nil {
			continue
		}

		kind, err := NewPermissionSubjectKind(subject.Kind)
		if err != nil {
			return nil, fmt.Errorf("failed to map kind with index: %d. %s", index, err)
		}

		result = append(result, PermissionSubject{
			Name: subject.Name,
			Kind: kind,
		})
	}

	return result, nil
}

// NewPermissionSubjectsFromOwners creates Subjects from user and group owners. Subjects will
// have User subjects that contains all users specified in users parameter and Group subjects
// that contains all groups specified in groups parameter. This function is used for backward
// compatibility when permission subjects didn't exist and only user and group owners could
// be associated with the queues.
func NewPermissionSubjectsFromOwners(users, groups []string) PermissionSubjects {
	subjects := make(PermissionSubjects, len(users)+len(groups))

	for index, user := range users {
		subjects[index] = PermissionSubject{
			Name: user,
			Kind: PermissionSubjectKindUser,
		}
	}

	for index, group := range groups {
		subjects[index+len(users)] = PermissionSubject{
			Name: group,
			Kind: PermissionSubjectKindGroup,
		}
	}

	return subjects
}
