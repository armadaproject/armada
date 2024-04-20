package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func preemptCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "preempt",
		Short: "Prempt jobs in armada.",
		Long:  `Preempt jobs by queue, jobset and jobId.`,
		Args:  cobra.ExactArgs(0),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			jobId, _ := cmd.Flags().GetString("jobId")
			queue, _ := cmd.Flags().GetString("queue")
			jobSetId, _ := cmd.Flags().GetString("jobSet")
			return a.Preempt(queue, jobSetId, jobId)
		},
	}
	cmd.Flags().String("jobId", "", "job to cancel")
	if err := cmd.MarkFlagRequired("jobId"); err != nil {
		panic(err)
	}
	cmd.Flags().String("queue", "", "queue of the job to be cancelled")
	if err := cmd.MarkFlagRequired("queue"); err != nil {
		panic(err)
	}
	cmd.Flags().String("jobSet", "", "jobSet of the job to be cancelled")
	if err := cmd.MarkFlagRequired("jobSet"); err != nil {
		panic(err)
	}
	return cmd
}
