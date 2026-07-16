// Package configuration defines the configuration types for the Lookout
// server, including database, TLS, pruner and UI configuration. UIConfig is
// serialised to JSON and served to the Lookout UI frontend, where it must be
// kept in sync with the LookoutUiConfig TypeScript interface defined in
// internal/lookoutui/src/config/types.ts.
package configuration
