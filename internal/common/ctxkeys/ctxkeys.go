// Package ctxkeys provides typed context keys shared across packages to prevent
// collisions with plain string keys.
package ctxkeys

// ContextKey is a typed key for context values, preventing collisions with
// string keys from other packages.
type ContextKey string

const (
	// PrincipalKey stores the authenticated Principal in a context.
	PrincipalKey ContextKey = "principal"
	// UserKey stores the authenticated username in a context for logging.
	UserKey ContextKey = "user"
	// RequestIDKey stores the request ID in a context for logging.
	RequestIDKey ContextKey = "requestId"
)
