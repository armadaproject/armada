package cmd

import (
	"fmt"

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
		cancelExecutorCmd(),
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

func cancelExecutorCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "executor <executor> <priority_class_1> <priority_class_2> ...",
		Short: "Cancels jobs on executor.",
		Long:  `Cancels jobs on executor with provided executor name, priority classes and queues.`,
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

			return a.CancelOnExecutor(onExecutor, matchQueues, onPriorityClasses)
		},
	}

	cmd.Flags().StringSliceP("match-queues", "q", []string{}, "Cancel jobs on executor matching the specified queue names. If no queues are provided, jobs across all queues will be cancelled.")
	return cmd
}
