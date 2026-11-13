package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetJobCmd_HasOutputFlag(t *testing.T) {
	f := jobGetCmd().Flags().Lookup("output")

	require.NotNil(t, f)
	assert.Equal(t, "o", f.Shorthand)
}

func TestGetJobCmd_NeedsExactlyOneJobId(t *testing.T) {
	cmd := jobGetCmd()

	assert.Error(t, cmd.Args(cmd, nil))
	assert.NoError(t, cmd.Args(cmd, []string{"a"}))
	assert.Error(t, cmd.Args(cmd, []string{"a", "b"}))
}
