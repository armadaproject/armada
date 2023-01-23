package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func analyzeCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:     "analyze <queue> <jobSet>",
		Aliases: []string{"gen"},
		Short:   "Analyze job events in job set.",
		Args:    cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSetId := args[1]
			return a.Analyze(queue, jobSetId)
		},
	}
	return cmd
}
