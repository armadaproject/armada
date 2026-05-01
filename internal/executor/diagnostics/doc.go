// Package diagnostics provides a built-in catalog of user-facing hints for
// well-known job failure modes, identified by regex match against kubelet or
// container runtime error text.
//
// LookupHint returns a hint string for the first matching pattern, or "" when
// no pattern matches. Patterns are compiled into the binary; for deployment-
// specific guidance use the categorizer's errorCategories configuration.
package diagnostics
