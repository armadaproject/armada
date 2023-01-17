package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func resourcesCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "resources <queue> <jobSet>",
		Short: "Prints out maximum resource usage for individual jobs.",
		Long:  `Prints out maximum resource usage for individual jobs in job set.`,
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queueName := args[0]
			jobSetId := args[1]
			return a.Resources(queueName, jobSetId)
		},
	}
	return cmd
}
