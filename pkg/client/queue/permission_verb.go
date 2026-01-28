package queue

import (
	"encoding/json"
	"math/rand"
	"reflect"
)

type PermissionVerb string

const (
	PermissionVerbSubmit       PermissionVerb = "submit"
	PermissionVerbCancel       PermissionVerb = "cancel"
	PermissionVerbPreempt      PermissionVerb = "preempt"
	PermissionVerbReprioritize PermissionVerb = "reprioritize"
	PermissionVerbWatch        PermissionVerb = "watch"
)

// NewPermissionVerb returns PermissionVerb from input string.
func NewPermissionVerb(in string) PermissionVerb {
	return PermissionVerb(in)
}

// UnmarshalJSON is implementation of https://pkg.go.dev/encoding/json#Unmarshaler interface.
func (verb *PermissionVerb) UnmarshalJSON(data []byte) error {
	permissionVerb := ""
	if err := json.Unmarshal(data, &permissionVerb); err != nil {
		return err
	}

	out := NewPermissionVerb(permissionVerb)
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
// slice is transformed into PermissionVerb.
func NewPermissionVerbs(verbs []string) PermissionVerbs {
	result := make([]PermissionVerb, len(verbs))

	for index, verb := range verbs {
		result[index] = NewPermissionVerb(verb)
	}

	return result
}

// AllPermissionVerbs returns PermissionsVerbs containing all PermissionVerb values
func AllPermissionVerbs() PermissionVerbs {
	return []PermissionVerb{
		PermissionVerbSubmit,
		PermissionVerbCancel,
		PermissionVerbPreempt,
		PermissionVerbReprioritize,
		PermissionVerbWatch,
	}
}
