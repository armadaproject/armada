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
	cmd.AddCommand(
		preemptJobCmd(),
		preemptExecutorCmd(),
	)
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
	return cmd
}

func preemptExecutorCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "executor <executor> <priority_class_1> <priority_class_2> ...",
		Short: "Preempts jobs on executor.",
		Long:  `Preempts jobs on executor with provided executor name, priority classes and queues.`,
		Args:  cobra.MinimumNArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			onExecutor := args[0]
			onPriorityClasses := args[1:]

			matchQueues, err := cmd.Flags().GetStringSlice("match-queues")
			if err != nil {
				return fmt.Errorf("error reading queue selection: %s", err)
			}

			return a.PreemptOnExecutor(onExecutor, matchQueues, onPriorityClasses)
		},
	}

	cmd.Flags().StringSliceP("match-queues", "q", []string{}, "Preempt jobs on executor matching the specified queue names. If no queues are provided, jobs across all queues will be preempted.")
	return cmd
}
