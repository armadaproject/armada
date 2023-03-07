package scheduler

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// TabWriter is a wrapper around a *tabwriter.Writer that allows for efficiently building
// tab-aligned strings.
// This exists as *tabwriter.Writer exposes a more complicated interface which returns errors to the
// caller, propagated for the underlying IOWriter.  In this case we ensure that the underlying Writer is
// a strings.Builder. This never returns errors therefore we can provide a simpler interface where the caller
// doesn't need to consider error handling.
type TabWriter struct {
	sb     *strings.Builder
	writer *tabwriter.Writer
}

// NewTabWriter creates a new TabWriter.  ALl parameters are equivalent to those defined in tabwriter.NewWriter
func NewTabWriter(minwidth, tabwidth, padding int, padchar byte, flags uint) *TabWriter {
	sb := &strings.Builder{}
	return &TabWriter{
		sb:     sb,
		writer: tabwriter.NewWriter(sb, minwidth, tabwidth, padding, padchar, flags),
	}
}

// Writef formats according to a format specifier and writes to the underlying writer
func (t *TabWriter) Writef(format string, a ...any) {
	// should be safe ignore the error here as strings.Builder never errors
	_, _ = fmt.Fprintf(t.writer, format, a...)
}

// String returns the accumulated string.
// Flush on the underlying writer is automatically called
func (t *TabWriter) String() string {
	// should be safe ignore the error here as strings.Builder never errors
	_ = t.writer.Flush()
	return t.sb.String()
}
