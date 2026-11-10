package cmd

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/pkg/api"
)

func TestGetCommands_HaveOutputFlag(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{
		"get queue":          queueGetCmd(),
		"get queues":         queuesGetCmd(),
		"get retry-policy":   retryPolicyGetCmd(),
		"get retry-policies": retryPolicyGetAllCmd(),
	} {
		f := cmd.Flags().Lookup("output")
		require.NotNil(t, f, name)
		assert.Equal(t, "o", f.Shorthand, name)
		assert.Contains(t, f.Usage, "yaml or json", name)
	}

	// Commands that only report what they did have no document to format.
	assert.Nil(t, RootCmd().PersistentFlags().Lookup("output"))
	assert.Nil(t, queueCreateCmd().Flags().Lookup("output"))
	assert.Nil(t, retryPolicyDeleteCmd().Flags().Lookup("output"))
}

func TestGetCommands_CompleteOutputFormats(t *testing.T) {
	cmd := queueGetCmd()
	completions, ok := cmd.GetFlagCompletionFunc("output")
	require.True(t, ok)
	got, directive := completions(cmd, nil, "")
	assert.Equal(t, []string{"yaml", "json"}, got)
	assert.Equal(t, cobra.ShellCompDirectiveNoFileComp, directive)
}

func TestOutputFlag_AppliesFormat(t *testing.T) {
	tests := map[string]struct {
		args     []string
		wantJson bool
	}{
		"json":             {args: []string{"-o", "json"}, wantJson: true},
		"long flag":        {args: []string{"--output", "json"}, wantJson: true},
		"case-insensitive": {args: []string{"-o", "JSON"}, wantJson: true},
		"yaml":             {args: []string{"-o", "yaml"}},
		"default":          {args: nil},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			out := &bytes.Buffer{}
			a.Out = out
			a.Params.QueueAPI.Get = func(name string) (*api.Queue, error) {
				return &api.Queue{Name: name}, nil
			}
			// A command shaped like the real ones, without initParams (which needs config and
			// would replace the stubbed API).
			cmd := withOutputFlag(&cobra.Command{
				Use:  "queue <name>",
				Args: cobra.ExactArgs(1),
				RunE: func(cmd *cobra.Command, args []string) error { return a.GetQueue(args[0]) },
			}, a)
			cmd.SetArgs(append([]string{"q1"}, tc.args...))
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			require.NoError(t, cmd.Execute())

			var parsed map[string]any
			err := json.Unmarshal(out.Bytes(), &parsed)
			if tc.wantJson {
				require.NoError(t, err, out.String())
				assert.Equal(t, "q1", parsed["name"])
				assert.Equal(t, "Queue", parsed["kind"])
			} else {
				assert.Error(t, err, "expected yaml, got json: %s", out.String())
				assert.Contains(t, out.String(), "name: q1")
			}
		})
	}
}

// The value is validated before initParams runs, so a bad value must fail without loading any config
// or touching the connection details.
func TestOutputFlag_InvalidValueFailsBeforeInit(t *testing.T) {
	for name, build := range map[string]func(*armadactl.App) *cobra.Command{
		"get queue":  queueGetCmdWithApp,
		"get queues": queuesGetCmdWithApp,
	} {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			args := []string{"-o", "xml"}
			if name == "get queue" {
				args = append(args, "q1")
			}
			cmd := build(a)
			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)

			err := cmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), `invalid output format "xml"`)
			assert.Nil(t, a.Params.ApiConnectionDetails, "initParams must not have run")
		})
	}

	// The retry policy commands build their own app, so only the error is observable.
	for name, cmd := range map[string]*cobra.Command{
		"get retry-policy":   retryPolicyGetCmd(),
		"get retry-policies": retryPolicyGetAllCmd(),
	} {
		t.Run(name, func(t *testing.T) {
			args := []string{"-o", "xml"}
			if name == "get retry-policy" {
				args = append(args, "p1")
			}
			cmd.SetArgs(args)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			err := cmd.Execute()
			require.Error(t, err)
			assert.Contains(t, err.Error(), `invalid output format "xml"`)
		})
	}
}
