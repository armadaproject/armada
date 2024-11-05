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
		Use:   "executor <executor>",
		Short: "Preempts jobs on executor.",
		Long:  `Preempts jobs on executor with provided executor name, priority classes and queues.`,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.MarkFlagRequired("priority-classes"); err != nil {
				return fmt.Errorf("error marking priority-class flag as required: %s", err)
			}
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			onExecutor := args[0]

			priorityClasses, err := cmd.Flags().GetStringSlice("priority-classes")
			if err != nil {
				return fmt.Errorf("error reading priority-class selection: %s", err)
			}

			queues, err := cmd.Flags().GetStringSlice("queues")
			if err != nil {
				return fmt.Errorf("error reading queue selection: %s", err)
			}

			return a.PreemptOnExecutor(onExecutor, queues, priorityClasses)
		},
	}
	cmd.Flags().StringSliceP(
		"queues",
		"q",
		[]string{},
		"Preempt jobs on executor matching the specified queue names. If no queues are provided, jobs across all queues will be preempted. Provided queues should be comma separated, as in the following example: queueA,queueB,queueC.",
	)
	cmd.Flags().StringSliceP("priority-classes", "p", []string{}, "Preempt jobs on executor matching the specified priority classes. Provided priority classes should be comma separated, as in the following example: armada-default,armada-preemptible.")
	return cmd
}
