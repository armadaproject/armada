package cmd

import (
	"github.com/spf13/cobra"

	"github.com/G-Research/armada/internal/armadactl"
)

func submitCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "submit ./path/to/jobs.yaml",
		Short: "Submit jobs to armada",
		Long: `Submit jobs to armada from file.

Example jobs.yaml:

jobs:
- queue: test
	priority: 0
	jobSetId: set1
	podSpec:
	... kubernetes pod spec ...`,
		Args: cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			path := args[0]
			return a.Submit(path)
		},
	}

	return cmd
}
