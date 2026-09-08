package observe

import (
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Error marks the span with an error and returns the error. It records the error in
// the span and sets the span's status to error with the error message. This function
// is useful for propagating errors while also ensuring that they are logged in the tracing system.
func Error(span trace.Span, err error) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return err
}

// Errorf formats an error message, marks the span with the error, and returns the formatted error.
// It is a convenience function that combines error formatting and span error recording in one step.
func Errorf(span trace.Span, format string, args ...any) error {
	return Error(span, fmt.Errorf(format, args...))
}
