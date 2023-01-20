package queue

import (
	"fmt"

	"github.com/armadaproject/armada/pkg/api"
)

// Permissions specifies the list of subjects that are allowed to perform queue operations
// specified by the list of verbs.
type Permissions struct {
	Subjects PermissionSubjects `json:"subjects"`
	Verbs    PermissionVerbs    `json:"verbs"`
}

// NewPermissionsFromOwners creates Permissions from user and group owners. Permissions will
// have User subjects that contains all users specified in users parameter and Group subjects
// that contains all groups specified in groups parameter. Permissions will also include all
// currently supported verbs (effectively emulating old user's and group's owner permissions).
// This function is used for backward compatibility when permissions didn't exist and only user
// and group owners could perform queue operations.
func NewPermissionsFromOwners(users, groups []string) Permissions {
	return Permissions{
		Subjects: NewPermissionSubjectsFromOwners(users, groups),
		Verbs:    AllPermissionVerbs(),
	}
}

// NewPermissions returns Permissions from *api.Queue_Permissions. An error is returned
// if subjects/verbs can't be generated from *api.Queue_Permissions.Subjects/*api.Queue_Permissions.Verbs
func NewPermissions(perm *api.Queue_Permissions) (Permissions, error) {
	if perm == nil {
		return Permissions{}, nil
	}

	subjects, err := NewPermissionSubjects(perm.Subjects)
	if err != nil {
		return Permissions{}, fmt.Errorf("failed to map subjects. %s", err)
	}

	verbs, err := NewPermissionVerbs(perm.Verbs)
	if err != nil {
		return Permissions{}, fmt.Errorf("failed to map verbs. %s", err)
	}

	return Permissions{
		Subjects: subjects,
		Verbs:    verbs,
	}, nil
}

// ToAPI converts Permissions to *api.Queue_Permissions.
func (p Permissions) ToAPI() *api.Queue_Permissions {
	verbs := make([]string, len(p.Verbs))
	for index, verb := range p.Verbs {
		verbs[index] = string(verb)
	}

	subjects := make([]*api.Queue_Permissions_Subject, len(p.Subjects))
	for index, subject := range p.Subjects {
		subjects[index] = &api.Queue_Permissions_Subject{
			Kind: string(subject.Kind),
			Name: subject.Name,
		}
	}

	return &api.Queue_Permissions{
		Subjects: subjects,
		Verbs:    verbs,
	}
}
