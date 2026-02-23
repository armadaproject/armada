package repository

import (
	"errors"
)

// ErrNotFound is returned by repository methods when the requested entity
// does not exist in the database.
var ErrNotFound = errors.New("not found")
