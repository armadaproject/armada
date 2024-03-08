package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
)

type PermissionSubjectKind string

const (
	PermissionSubjectKindGroup PermissionSubjectKind = "Group"
	PermissionSubjectKindUser  PermissionSubjectKind = "User"
)

// NewPermissionSubjectKind returns PermissionSubjectKind from input string. If input string
// doesn't match one of the allowed kind values ["Group", "User"] error is returned.
func NewPermissionSubjectKind(in string) (PermissionSubjectKind, error) {
	switch kind := PermissionSubjectKind(in); kind {
	case PermissionSubjectKindUser, PermissionSubjectKindGroup:
		return kind, nil
	default:
		return "", fmt.Errorf("invalid kind value: %s", in)
	}
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
//
// TODO: Unused.
func (kind *PermissionSubjectKind) UnmarshalJSON(data []byte) error {
	subjectKind := ""
	if err := json.Unmarshal(data, &subjectKind); err != nil {
		return err
	}

	out, err := NewPermissionSubjectKind(subjectKind)
	if err != nil {
		return fmt.Errorf("invalid queue permission subject kind. %s", err)
	}

	*kind = out
	return nil
}

// Generate is implementation of https://pkg.go.dev/testing/quick#Generator interface.
// This method is used for writing tests usign https://pkg.go.dev/testing/quick package.
//
// TODO: Unused.
func (kind PermissionSubjectKind) Generate(rand *rand.Rand, size int) reflect.Value {
	values := []PermissionSubjectKind{
		PermissionSubjectKindUser,
		PermissionSubjectKindGroup,
	}

	return reflect.ValueOf(values[rand.Intn(len(values))])
}
