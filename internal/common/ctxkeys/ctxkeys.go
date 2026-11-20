// Package ctxkeys defines the keys under which Armada stores values in a context.Context.
// It has no dependencies so that any package can use it without creating an import cycle.
package ctxkeys

// contextKey is unexported so that keys from other packages can never collide with these,
// even if they have the same underlying string value.
type contextKey string

const (
	// PrincipalKey is the key used to store the authenticated principal.
	PrincipalKey contextKey = "principal"
	// UserKey is the key used to store the user name for request logging.
	UserKey contextKey = "user"
	// RequestIDKey is the key used to store the request id for request logging.
	RequestIDKey contextKey = "requestId"
)
