package logging

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const Stacktrace = "stacktrace"

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

// Unexported but considered part of the stable interface of pkg/errors.
type causer interface {
	Cause() error
}

// WithStacktrace returns a new logrus.Entry obtained by adding error information and, if available, a stack trace
// as fields to the provided logrus.Entry.
func WithStacktrace(logger *logrus.Entry, err error) *logrus.Entry {
	logger = logger.WithError(err)
	stack := ExtractStack(err)
	if stack != nil {
		logger = logger.WithField(Stacktrace, stack)
	}
	return logger
}

// ExtractStack walks down the list of errors and retrieves the first errors.StackTrace it encounters
// If no stacktraces are found, it returns nil
func ExtractStack(err error) errors.StackTrace {
	if stackErr, ok := err.(stackTracer); ok {
		return stackErr.StackTrace()
	} else if causeErr, ok := err.(causer); ok {
		return ExtractStack(causeErr.Cause())
	}
	return nil
}
