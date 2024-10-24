package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func preemptCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preempt",
		Short: "Preempt jobs in armada.",
		Args:  cobra.ExactArgs(0),
	}
	cmd.Flags().String("reason", "", "Reason for preemption")
	cmd.AddCommand(preemptJobCmd())
	return cmd
}

func preemptJobCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "job <queue> <job-set> <job-id>",
		Short: "Preempt an armada job.",
		Long:  `Preempt a job by providing it's queue, jobset and jobId.`,
		Args:  cobra.ExactArgs(3),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			queue := args[0]
			jobSetId := args[1]
			jobId := args[2]
			reason, err := cmd.Flags().GetString("reason")
			if err != nil {
				return fmt.Errorf("error reading reason: %s", err)
			}
			return a.Preempt(queue, jobSetId, jobId, reason)
		},
	}
	cmd.Flags().String("reason", "", "Reason for preemption")
	return cmd
}
