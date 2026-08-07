package observe

import (
	"fmt"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func Error(span trace.Span, err error) error {
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	return err
}

func Errorf(span trace.Span, format string, args ...any) error {
	return Error(span, fmt.Errorf(format, args...))
}
