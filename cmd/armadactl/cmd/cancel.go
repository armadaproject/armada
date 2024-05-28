package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func cancelCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancels jobs in armada.",
		Long:  `Cancels jobs individually using job ID or in bulk as part of a job set.`,
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		cancelJobCmd(),
		cancelJobSetCmd(),
	)
	return cmd
}

func cancelJobCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "job <queue> <job-set> <job-id>",
		Short: "Cancels job in armada.",
		Long:  `Cancel job by providing queue, job-set and job-id.`,
		Args:  cobra.ExactArgs(3),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSetId := args[1]
			jobId := args[2]
			return a.CancelJob(queue, jobSetId, jobId)
		},
	}
	return cmd
}

func cancelJobSetCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "job-set <queue> <job-set>",
		Short: "Cancels job-set in armada.",
		Long:  `Cancels job-set by providing queue, job-set.`,
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSetId := args[1]
			return a.CancelJobSet(queue, jobSetId)
		},
	}
	return cmd
}
