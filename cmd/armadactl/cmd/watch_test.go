package cmd

import (
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
)

func TestWatch(t *testing.T) {
	// TODO there are no tests for invalid input because cobra silently discards those inputs without raising errors
	tests := map[string]struct {
		Flags               []flag
		raw                 bool
		exit_if_inactive    bool
		force_new_events    bool
		force_legacy_events bool
	}{
		"default flags":             {nil, false, false, false, false},
		"valid raw":                 {[]flag{{"raw", "true"}}, true, false, false, false},
		"valid exit-if-inactive":    {[]flag{{"exit-if-inactive", "true"}}, false, true, false, false},
		"valid force-new-events":    {[]flag{{"force-new-events", "true"}}, false, false, true, false},
		"valid force-legacy-events": {[]flag{{"force-legacy-events", "true"}}, false, false, false, true},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := watchCmd()
			for _, flag := range test.Flags {
				require.NoError(t, cmd.Flags().Set(flag.name, flag.value))
			}
			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Out = io.Discard
				if test.raw {
					r, err := cmd.Flags().GetBool("raw")
					require.NoError(t, err)
					require.Equal(t, test.raw, r)
				}
				if test.exit_if_inactive {
					exitOnInactiveFlag, err := cmd.Flags().GetBool("exit-if-inactive")
					require.NoError(t, err)
					require.Equal(t, test.raw, exitOnInactiveFlag)
				}
				if test.force_new_events {
					forceNewEventsFlag, err := cmd.Flags().GetBool("force-new-events")
					require.NoError(t, err)
					require.Equal(t, test.raw, forceNewEventsFlag)
				}
				if test.force_legacy_events {
					forceLegacyEventsFlag, err := cmd.Flags().GetBool("force-legacy-events")
					require.NoError(t, err)
					require.Equal(t, test.raw, forceLegacyEventsFlag)
				}
				return nil
			}
			cmd.SetArgs([]string{"arbitrary", "jobSetId1"})
		})
	}
}
