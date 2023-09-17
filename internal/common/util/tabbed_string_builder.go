package util

import (
	"fmt"
	"strings"
	"text/tabwriter"
)

// TabbedStringBuilder is a wrapper around a *tabwriter.Writer that allows for efficiently building
// tab-aligned strings.
// This exists as *tabwriter.Writer exposes a more complicated interface which returns errors to the
// caller, propagated for the underlying IOWriter.  In this case we ensure that the underlying Writer is
// a strings.Builder. This never returns errors therefore we can provide a simpler interface where the caller
// doesn't need to consider error handling.
type TabbedStringBuilder struct {
	sb     *strings.Builder
	writer *tabwriter.Writer
}

// NewTabbedStringBuilder creates a new TabbedStringBuilder.  All parameters are equivalent to those defined in tabwriter.NewWriter
func NewTabbedStringBuilder(minwidth, tabwidth, padding int, padchar byte, flags uint) *TabbedStringBuilder {
	sb := &strings.Builder{}
	return &TabbedStringBuilder{
		sb:     sb,
		writer: tabwriter.NewWriter(sb, minwidth, tabwidth, padding, padchar, flags),
	}
}

// Writef formats according to a format specifier and writes to the underlying writer
func (t *TabbedStringBuilder) Writef(format string, a ...any) {
	// should be safe ignore the error here as strings.Builder never errors
	_, _ = fmt.Fprintf(t.writer, format, a...)
}

// Write the string to the underlying writer
func (t *TabbedStringBuilder) Write(a ...any) {
	// should be safe ignore the error here as strings.Builder never errors
	_, _ = fmt.Fprint(t.writer, a...)
}

// String returns the accumulated string.
// Flush on the underlying writer is automatically called
func (t *TabbedStringBuilder) String() string {
	// should be safe ignore the error here as strings.Builder never errors
	_ = t.writer.Flush()
	return t.sb.String()
}
