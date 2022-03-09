package logging

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

// WithStacktrace returns a new logrus.Entry obtained by adding error information and, if available, a stack trace
// as fields to the provided logrus.Entry.
func WithStacktrace(logger *logrus.Entry, err error) *logrus.Entry {
	logger = logger.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.WithField("stacktrace", stackErr.StackTrace())
	} else {
		return logger
	}
}
