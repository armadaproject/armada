// Package version exposes the Lookout backend's release version, commit, and
// build time. The variables are populated at link time via -ldflags during
// release builds; in development builds they retain their default sentinel
// values ("dev", "unknown").
package version
