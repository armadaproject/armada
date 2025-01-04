package logging

import (
	"github.com/pkg/errors"
)

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

// WithStacktrace returns a new Logger obtained by adding error information and, if available, a stack trace
// as fields
func WithStacktrace(logger *Logger, err error) *Logger {
	logger = logger.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.With("stacktrace", stackErr.StackTrace())
	} else {
		return logger
	}
}
