package cmd

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// noopRunE disables server calls so we can test argument parsing only.
func noopExecCmd() *cobra.Command {
	cmd := execCmd()
	cmd.PreRunE = nil
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return nil }
	return cmd
}

func TestExecCmd_NoJobID(t *testing.T) {
	cmd := noopExecCmd()
	cmd.SetArgs([]string{"--", "/bin/bash"})
	err := cmd.Execute()
	// "--" ends up with /bin/bash as the only arg → no job ID
	assert.Error(t, err)
}

func TestExecCmd_NoCommand(t *testing.T) {
	cmd := noopExecCmd()
	cmd.SetArgs([]string{"abc123"})
	err := cmd.Execute()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "command")
}

func TestExecCmd_BasicFlags(t *testing.T) {
	var (
		capturedJobID  string
		capturedCmd    []string
	)

	cmd := execCmd()
	cmd.PreRunE = nil
	cmd.RunE = func(c *cobra.Command, args []string) error {
		capturedJobID = args[0]
		capturedCmd = args[1:]
		return nil
	}
	cmd.SetArgs([]string{"abc123", "--", "/bin/bash"})
	require.NoError(t, cmd.Execute())

	assert.Equal(t, "abc123", capturedJobID)
	assert.Equal(t, []string{"/bin/bash"}, capturedCmd)
}

func TestExecCmd_ContainerFlag(t *testing.T) {
	var container string

	cmd := execCmd()
	cmd.PreRunE = nil
	cmd.RunE = func(c *cobra.Command, args []string) error {
		container, _ = c.Flags().GetString("container")
		return nil
	}
	cmd.SetArgs([]string{"abc123", "-c", "sidecar", "--", "cat", "/tmp/foo"})
	require.NoError(t, cmd.Execute())

	assert.Equal(t, "sidecar", container)
}

func TestExecCmd_PodFlag(t *testing.T) {
	var podNumber int32

	cmd := execCmd()
	cmd.PreRunE = nil
	cmd.RunE = func(c *cobra.Command, args []string) error {
		p, _ := c.Flags().GetInt32("pod")
		podNumber = p
		return nil
	}
	cmd.SetArgs([]string{"abc123", "--pod", "2", "--", "sh"})
	require.NoError(t, cmd.Execute())

	assert.Equal(t, int32(2), podNumber)
}
