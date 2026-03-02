// Package logging provides structured logging utilities for Armada components.
//
// It wraps zerolog and exposes a global logger configurable at application
// startup via [ConfigureApplicationLogging] or [MustConfigureApplicationLogging].
// The [NewFileWriter] and [NewStdoutWriter] functions allow callers to tee log
// output to a file alongside the default console writer.
package logging
