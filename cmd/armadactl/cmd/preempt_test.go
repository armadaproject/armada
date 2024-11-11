package cmd

import (
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
)

func TestPreemptQueue(t *testing.T) {
	tests := map[string]struct {
		Flags           []flag
		selectors       []string
		priorityClasses []string
		inverse         bool
		onlyCordoned    bool
		dryRun          bool
	}{
		"default flags":            {nil, []string{}, []string{}, false, false, false},
		"valid selectors":          {[]flag{{"selectors", "armadaproject.io/priority=high,armadaproject.io/category=critical"}}, []string{"armadaproject.io/priority=high", "armadaproject.io/category=critical"}, []string{}, false, false, false},
		"valid priority-classes 1": {[]flag{{"priority-classes", "armada-default"}}, []string{}, []string{"armada-default"}, false, false, false},
		"valid priority-classes 2": {[]flag{{"priority-classes", "armada-default,armada-preemptible"}}, []string{}, []string{"armada-default", "armada-preemptible"}, false, false, false},
		"valid multiple flags": {
			[]flag{{"selectors", "armadaproject.io/priority=high,armadaproject.io/category=critical"}, {"priority-classes", "armada-default,armada-preemptible"}},
			[]string{"armadaproject.io/priority=high", "armadaproject.io/category=critical"},
			[]string{"armada-default", "armada-preemptible"},
			true, true, true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := preemptQueuesCmd()

			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Out = io.Discard

				if len(test.selectors) > 0 {
					selectorsFlag, err := cmd.Flags().GetString("selectors")
					require.Error(t, err)
					require.Equal(t, test.selectors, selectorsFlag)
				}
				if len(test.priorityClasses) > 0 {
					priorityClassesFlag, err := cmd.Flags().GetString("priority-classes")
					require.Error(t, err)
					require.Equal(t, test.priorityClasses, priorityClassesFlag)
				}

				inverseValue, err := cmd.Flags().GetBool("inverse")
				require.NoError(t, err)
				require.Equal(t, test, inverseValue)

				onlyCordonedValue, err := cmd.Flags().GetBool("only-cordoned")
				require.NoError(t, err)
				require.Equal(t, test, onlyCordonedValue)

				dryRunValue, err := cmd.Flags().GetBool("dry-run")
				require.NoError(t, err)
				require.Equal(t, test, dryRunValue)

				return nil
			}
		})
	}
}
