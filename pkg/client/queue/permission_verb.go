package queue

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
)

type PermissionVerb string

const (
	PermissionVerbSubmit       PermissionVerb = "submit"
	PermissionVerbCancel       PermissionVerb = "cancel"
	PermissionVerbReprioritize PermissionVerb = "reprioritize"
	PermissionVerbWatch        PermissionVerb = "watch"
)

// NewPermissionVerb returns PermissionVerb from input string. If input string doesn't match
// one of allowed verb values ["submit", "cancel", "reprioritize", "watch"], and error is returned.
func NewPermissionVerb(in string) (PermissionVerb, error) {
	switch verb := PermissionVerb(in); verb {
	case PermissionVerbSubmit, PermissionVerbCancel, PermissionVerbReprioritize, PermissionVerbWatch:
		return verb, nil
	default:
		return "", fmt.Errorf("invalid queue permission verb: %s", in)
	}
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
func (verb *PermissionVerb) UnmarshalJSON(data []byte) error {
	permissionVerb := ""
	if err := json.Unmarshal(data, &permissionVerb); err != nil {
		return err
	}

	out, err := NewPermissionVerb(permissionVerb)
	if err != nil {
		return fmt.Errorf("failed to unmarshal queue permission verb: %s", err)
	}

	*verb = out
	return nil
}

// Generate is implementation of https://pkg.go.dev/testing/quick#Generator interface.
// This method is used for writing tests usign https://pkg.go.dev/testing/quick package
func (verb PermissionVerb) Generate(rand *rand.Rand, size int) reflect.Value {
	values := AllPermissionVerbs()
	return reflect.ValueOf(values[rand.Intn(len(values))])
}

type PermissionVerbs []PermissionVerb

// NewPermissionVerbs returns PermissionVerbs from string slice. Every string from
// slice is transformed into PermissionVerb. Error is returned if a string cannot
// be transformed to PermissionVerb.
func NewPermissionVerbs(verbs []string) (PermissionVerbs, error) {
	result := make([]PermissionVerb, len(verbs))

	for index, verb := range verbs {
		validVerb, err := NewPermissionVerb(verb)
		if err != nil {
			return nil, fmt.Errorf("failed to map verb string with index: %d. %s", index, err)
		}

		result[index] = validVerb
	}

	return result, nil
}

// AllPermissionVerbs returns PermissionsVerbs containing all PermissionVerb values
func AllPermissionVerbs() PermissionVerbs {
	return []PermissionVerb{
		PermissionVerbSubmit,
		PermissionVerbCancel,
		PermissionVerbReprioritize,
		PermissionVerbWatch,
	}
}
