package client

import (
	"io"
	"os"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureStd runs f and returns what it wrote to stdout and stderr.
func captureStd(t *testing.T, f func()) (stdout, stderr string) {
	t.Helper()
	origOut, origErr := os.Stdout, os.Stderr
	outR, outW, err := os.Pipe()
	require.NoError(t, err)
	errR, errW, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout, os.Stderr = outW, errW
	defer func() { os.Stdout, os.Stderr = origOut, origErr }()

	f()

	require.NoError(t, outW.Close())
	require.NoError(t, errW.Close())
	o, err := io.ReadAll(outR)
	require.NoError(t, err)
	e, err := io.ReadAll(errR)
	require.NoError(t, err)
	return string(o), string(e)
}

// The warning must not reach stdout: armadactl's --output json/yaml documents are written there, and
// a warning in front of them would make them unparseable.
func TestExtractConnectionDetails_NoContextWarningGoesToStderr(t *testing.T) {
	viper.Reset()
	t.Cleanup(viper.Reset)

	var details *ApiConnectionDetails
	stdout, stderr := captureStd(t, func() {
		var err error
		details, err = ExtractCommandlineArmadaApiConnectionDetails()
		require.NoError(t, err)
	})

	assert.NotNil(t, details)
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "No context defined")
}
