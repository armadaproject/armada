package logging

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Unexported but considered part of the stable interface of pkg/errors.
type stackTracer interface {
	StackTrace() errors.StackTrace
}

// WithStacktrace returns a new logrus.FieldLogger obtained by adding error information and, if available, a stack trace
// as fields to the provided logrus.FieldLogger.
func WithStacktrace(logger logrus.FieldLogger, err error) logrus.FieldLogger {
	logger = logger.WithError(err)
	if stackErr, ok := err.(stackTracer); ok {
		return logger.WithField("stacktrace", stackErr.StackTrace())
	} else {
		return logger
	}
}
