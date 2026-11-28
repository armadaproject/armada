package client

import (
	"io"
	"os"
	"testing"
	"time"

	"github.com/spf13/cobra"
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
	t.Cleanup(func() { _ = outR.Close(); _ = outW.Close() })
	errR, errW, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = errR.Close(); _ = errW.Close() })
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

// The lookoutUrl flag has a default, which must not replace the url a context has saved.
func TestExtractConnectionDetails_LookoutUrl(t *testing.T) {
	const saved = "https://lookout.remote.example.com"
	tests := map[string]struct {
		savedInContext bool
		flag           string // passed as --lookoutUrl when not empty
		want           string
	}{
		"the saved url is kept when the flag is not passed": {savedInContext: true, want: saved},
		"the flag replaces the saved url":                   {savedInContext: true, flag: "localhost:9999", want: "localhost:9999"},
		"the flag replaces the saved url, even as the default": {
			savedInContext: true, flag: defaultLookoutUrl, want: defaultLookoutUrl,
		},
		"the default is used when the context has no url": {want: defaultLookoutUrl},
		"the flag is used when the context has no url":    {flag: "localhost:9999", want: "localhost:9999"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			cmd := &cobra.Command{}
			AddArmadaApiConnectionCommandlineArgs(cmd)
			viper.Set("currentContext", "remote")
			viper.Set("contexts.remote.armadaUrl", "armada.remote.example.com:443")
			if tc.savedInContext {
				viper.Set("contexts.remote.lookoutUrl", saved)
			}
			if tc.flag != "" {
				require.NoError(t, cmd.PersistentFlags().Set("lookoutUrl", tc.flag))
			}

			details, err := ExtractCommandlineArmadaApiConnectionDetails()

			require.NoError(t, err)
			assert.Equal(t, tc.want, details.LookoutUrl)
			assert.Equal(t, "armada.remote.example.com:443", details.ArmadaUrl)
		})
	}
}

// The lookoutTimeout flag has no default, so a context's saved timeout is only replaced by a flag that is passed.
func TestExtractConnectionDetails_LookoutTimeout(t *testing.T) {
	tests := map[string]struct {
		saved string // the timeout saved in the context, if not empty
		flag  string // passed as --lookoutTimeout when not empty
		want  time.Duration
	}{
		"zero when nothing is set":                              {want: 0},
		"the saved timeout is kept when the flag is not passed": {saved: "90s", want: 90 * time.Second},
		"the flag replaces the saved timeout":                   {saved: "90s", flag: "2m", want: 2 * time.Minute},
		"the flag is used when the context has none":            {flag: "30s", want: 30 * time.Second},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			viper.Reset()
			t.Cleanup(viper.Reset)
			cmd := &cobra.Command{}
			AddArmadaApiConnectionCommandlineArgs(cmd)
			viper.Set("currentContext", "remote")
			viper.Set("contexts.remote.armadaUrl", "armada.remote.example.com:443")
			if tc.saved != "" {
				viper.Set("contexts.remote.lookoutTimeout", tc.saved)
			}
			if tc.flag != "" {
				require.NoError(t, cmd.PersistentFlags().Set("lookoutTimeout", tc.flag))
			}

			details, err := ExtractCommandlineArmadaApiConnectionDetails()

			require.NoError(t, err)
			assert.Equal(t, tc.want, details.LookoutTimeout)
		})
	}
}
