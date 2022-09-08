package util

import (
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/oklog/ulid"
)

var (
	entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	m       sync.Mutex
)

// NewULID returns a new ULID for the current time generated from a global RNG.
// The ULID is returned as a string converted to lower-case to ensure it is a valid DNS subdomain name; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
func NewULID() string {
	return StringFromUlid(ULID())
}

// StringFromUlid returns a string representation of a proto UUID.
// Because Kubernetes requires ids to be valid DNS subdomain names, the string is returned in lower-case; see
// https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-subdomain-names
func StringFromUlid(id ulid.ULID) string {
	return strings.ToLower(id.String())
}

// ULID returns a new ULID for the current time generated from a global RNG.
func ULID() ulid.ULID {
	m.Lock()
	defer m.Unlock()
	id := ulid.MustNew(ulid.Now(), entropy)
	return id
}
