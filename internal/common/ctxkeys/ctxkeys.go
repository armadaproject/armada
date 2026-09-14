// Package ctxkeys provides typed context keys shared across packages to prevent
// collisions with plain string keys.
package ctxkeys

// contextKey is an unexported type for context value keys, preventing any
// external package from constructing a colliding key.
type contextKey string

const (
	// PrincipalKey stores the authenticated Principal in a context.
	PrincipalKey contextKey = "principal"
	// UserKey stores the authenticated username in a context for logging.
	UserKey contextKey = "user"
	// RequestIDKey stores the request ID in a context for logging.
	RequestIDKey contextKey = "requestId"
)
