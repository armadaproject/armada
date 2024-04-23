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
		Long:  `Cancels jobs.  If queue and jobset are provided then all jobs in that jobset will be cancelled.  A job id may also be provided in which case only that job is cancelled`,
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
	cmd.Flags().String("jobId", "", "job to cancel (optional)")
	cmd.Flags().String("queue", "", "queue to cancel jobs from")
	cmd.Flags().String("jobSet", "", "jobSet to cancel")
	cmd.MarkFlagRequired("queue")
	cmd.MarkFlagRequired("jobSet")
	return cmd
}
