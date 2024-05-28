package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func preemptCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "preempt",
		Short: "Preempt jobs in armada.",
		Args:  cobra.ExactArgs(0),
	}
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
			return a.Preempt(queue, jobSetId, jobId)
		},
	}
	cmd.Flags().String("jobId", "", "job to preempt")
	if err := cmd.MarkFlagRequired("jobId"); err != nil {
		panic(err)
	}
	cmd.Flags().String("queue", "", "queue of the job to be preempted")
	if err := cmd.MarkFlagRequired("queue"); err != nil {
		panic(err)
	}
	cmd.Flags().String("jobSet", "", "jobSet of the job to be preempted")
	if err := cmd.MarkFlagRequired("jobSet"); err != nil {
		panic(err)
	}
	return cmd
}
