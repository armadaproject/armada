package cmd

import (
	"io"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/armadaproject/armada/internal/armadactl"
)

func TestCancel(t *testing.T) {
	tests := map[string]struct {
		Flags  []flag
		jobId  string
		queue  string
		jobSet string
	}{
		"default flags": {nil, "", "", ""},
		"valid job-id":  {[]flag{{"job-id", "jobId1"}}, "jobId1", "", ""},
		"valid queue":   {[]flag{{"queue", "queue1,jobSet1"}}, "", "queue1", "jobSet1"},
		"valid job-set": {[]flag{{"job-set", "jobSet1"}}, "", "", "jobSet1"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelCmd()

			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Out = io.Discard

				if len(test.jobId) > 0 {
					jobIdFlag, err1 := cmd.Flags().GetString("job-id")
					require.Error(t, err1)
					require.Equal(t, test.jobId, jobIdFlag)
				}
				if len(test.queue) > 0 {
					queueFlag, err1 := cmd.Flags().GetString("queue")
					jobSetFlag, err2 := cmd.Flags().GetString("job-set")
					require.Error(t, err1)
					require.Error(t, err2)
					require.Equal(t, test.queue, queueFlag)
					require.Equal(t, test.jobSet, jobSetFlag)
				}
				if len(test.jobSet) > 0 {
					jobSetFlag, err1 := cmd.Flags().GetString("job-set")
					require.Error(t, err1)
					require.Equal(t, test.jobSet, jobSetFlag)
				}
				return nil
			}
		})
	}
}

func TestCancelQueue(t *testing.T) {
	tests := map[string]struct {
		Flags           []flag
		jobStates       []string
		selectors       []string
		priorityClasses []string
		inverse         bool
		onlyCordoned    bool
		dryRun          bool
	}{
		"default flags":            {nil, []string{}, []string{}, []string{}, false, false, false},
		"valid selectors":          {[]flag{{"selectors", "armadaproject.io/priority=high,armadaproject.io/category=critical"}}, []string{}, []string{"armadaproject.io/priority=high", "armadaproject.io/category=critical"}, []string{}, false, false, false},
		"valid job-states 1":       {[]flag{{"job-states", "queued"}}, []string{"queued"}, []string{}, []string{}, false, false, false},
		"valid job-states 2":       {[]flag{{"job-states", "queued,leased,pending,running"}}, []string{"queued", "leased", "pending", "running"}, []string{}, []string{}, false, false, false},
		"valid priority-classes 1": {[]flag{{"priority-classes", "armada-default"}}, []string{}, []string{}, []string{"armada-default"}, false, false, false},
		"valid priority-classes 2": {[]flag{{"priority-classes", "armada-default,armada-preemptible"}}, []string{}, []string{}, []string{"armada-default", "armada-preemptible"}, false, false, false},
		"valid multiple flags": {
			[]flag{{"selectors", "armadaproject.io/priority=high,armadaproject.io/category=critical"}, {"job-states", "queued,leased,pending,running"}, {"priority-classes", "armada-default,armada-preemptible"}},
			[]string{"queued", "leased", "pending", "running"},
			[]string{"armadaproject.io/priority=high", "armadaproject.io/category=critical"},
			[]string{"armada-default", "armada-preemptible"},
			true, true, true,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			a := armadactl.New()
			cmd := cancelQueueCmd()

			cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
				a.Out = io.Discard

				if len(test.jobStates) > 0 {
					jobStatesFlag, err := cmd.Flags().GetString("job-states")
					require.NoError(t, err)
					require.Equal(t, test.jobStates, jobStatesFlag)
				}
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
