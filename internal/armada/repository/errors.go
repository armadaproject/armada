package repository

import (
	"fmt"
	"strings"
)

// ErrNotFound should be returned by all repository functions whenever one or more
// resources can't be found.
//
// When constructing errors of this type, you need to prepend the resource type.
// For example, if a queue with name "foo" can't be found, create the error with
// &ErrNotFound{ResourceNames: []string{"queue \"foo\""}}
// instead of &ErrNotFound{ResourceNames: []string{"\"foo\""}}.
// Doing so makes it unambiguous to the user which resource is referred to.
//
// If several of the same resource are missing, add those as a single resource name, e.g.,
// &ErrNotFound{ResourceNames: []string{"jobs [\"1\", \"2\", \"3\"] \"foo\""}}.
//
// This error produces error messages of the form
// "could not find queue "foo"", or, if several resources are missing,
// "could not find any of [queue "foo", job "1234"]".
type ErrNotFound struct {
	ResourceNames []string
}

func (err *ErrNotFound) Error() string {
	if len(err.ResourceNames) == 0 {
		return "could not find <UNKNOWN>"
	} else if len(err.ResourceNames) == 1 {
		return fmt.Sprintf("could not find %q", err.ResourceNames[0])
	} else {
		return fmt.Sprintf("could not find any of [%s]", strings.Join(err.ResourceNames, ", "))
	}
}

// ErrNotFound should be returned by all repository functions whenever one or more
// resources to be created already exists.
//
// This error works, and should be constructed, in the same was as ErrNotFound.
type ErrAlreadyExists struct {
	ResourceNames []string
}

func (err *ErrAlreadyExists) Error() string {
	if len(err.ResourceNames) == 0 {
		return "resource <UNKNOWN> already exists"
	} else if len(err.ResourceNames) == 1 {
		return fmt.Sprintf("%q already exists", err.ResourceNames[0])
	} else {
		return fmt.Sprintf("the following already exists [%s]", strings.Join(err.ResourceNames, ", "))
	}
}
