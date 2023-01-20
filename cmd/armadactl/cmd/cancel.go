package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func cancelCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancels jobs in armada.",
		Long:  `Cancels jobs either by jobId or by combination of queue & job set.`,
		Args:  cobra.ExactArgs(0),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobId, _ := cmd.Flags().GetString("jobId")
			queue, _ := cmd.Flags().GetString("queue")
			jobSetId, _ := cmd.Flags().GetString("jobSet")
			return a.Cancel(queue, jobSetId, jobId)
		},
	}
	cmd.Flags().String("jobId", "", "job to cancel")
	cmd.Flags().String("queue", "", "queue to cancel jobs from (requires job set to be specified)")
	cmd.Flags().String("jobSet", "", "jobSet to cancel (requires queue to be specified)")
	return cmd
}
