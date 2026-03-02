package logging

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewFileWriter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")

	f, w, err := NewFileWriter(path)
	require.NoError(t, err)
	defer f.Close()

	event := []byte(`{"level":"debug","message":"should not appear"}` + "\n")
	_, writeErr := w.WriteLevel(zerolog.DebugLevel, event)
	assert.NoError(t, writeErr)

	f.Close()
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Empty(t, contents, "debug-level message should be filtered out")

	f2, w2, err := NewFileWriter(path)
	require.NoError(t, err)
	defer f2.Close()

	infoEvent := []byte(`{"level":"info","message":"hello"}` + "\n")
	_, writeErr = w2.WriteLevel(zerolog.InfoLevel, infoEvent)
	assert.NoError(t, writeErr)
	f2.Close()

	contents2, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.NotEmpty(t, contents2, "info-level message should be written")
	assert.Contains(t, string(contents2), "hello")
}

func TestNewStdoutWriter(t *testing.T) {
	w, err := NewStdoutWriter()
	require.NoError(t, err)
	assert.NotNil(t, w)
}
