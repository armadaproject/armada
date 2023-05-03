package cmd

import (
	"io"
	"testing"

	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestCancel(t *testing.T) {
	tests := map[string]struct {
		Flags  []flag
		jobId  string
		queue  string
		jobSet string
	}{
		"default flags": {nil, "", "", ""},
		"valid jobId":   {[]flag{{"jobId", "jobId1"}}, "jobId1", "", ""},
		"valid queue":   {[]flag{{"queue", "queue1,jobSet1"}}, "", "queue1", "jobSet1"},
		"valid jobSet":  {[]flag{{"jobSet", "jobSet1"}}, "", "", "jobSet1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelCmd()

			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Out = io.Discard

				if len(test.jobId) > 0 {
					jobIdFlag, err1 := cmd.Flags().GetString("jobId")
					require.Error(t, err1)
					require.Equal(t, test.jobId, jobIdFlag)
				}
				if len(test.queue) > 0 {
					queueFlag, err1 := cmd.Flags().GetString("queue")
					jobSetFlag, err2 := cmd.Flags().GetString("jobSet")
					require.Error(t, err1)
					require.Error(t, err2)
					require.Equal(t, test.queue, queueFlag)
					require.Equal(t, test.jobSet, jobSetFlag)
				}
				if len(test.jobSet) > 0 {
					jobSetFlag, err1 := cmd.Flags().GetString("jobSet")
					require.Error(t, err1)
					require.Equal(t, test.jobSet, jobSetFlag)
				}
				return nil
			}
		})
	}
}

