package logging

import (
	"io"

	"github.com/rs/zerolog"
)

type FilteredLevelWriter struct {
	writer io.Writer
	level  zerolog.Level
}

// Write writes to the underlying Writer.
func (w *FilteredLevelWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

// WriteLevel calls WriteLevel of the underlying Writer only if the level is equal
// or above the Level.
func (w *FilteredLevelWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	if level >= w.level {
		return w.writer.Write(p)
	}
	return len(p), nil
}
