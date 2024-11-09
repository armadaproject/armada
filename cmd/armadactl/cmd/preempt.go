package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/cmd/armadactl/cmd/utils"
	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/internal/common/slices"
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
		preemptQueuesCmd(),
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

func preemptQueuesCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "queues <queue_1> <queue_2> <queue_3> ...",
		Short: "Preempts jobs on queues.",
		Long:  `Preempts jobs on selected queues in specified priority classes. Allows selecting of queues by label or name, one of which must be provided. All flags with multiple values must be comma separated.`,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.MarkFlagRequired("priority-classes"); err != nil {
				return err
			}
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, queues []string) error {
			errs := slices.Filter(slices.Map(queues, utils.QueueNameValidation), func(err error) bool { return err != nil })
			if len(errs) > 0 {
				return fmt.Errorf("provided queue name invalid: %s", errs[0])
			}

			onlyCordoned, err := cmd.Flags().GetBool("only-cordoned")
			if err != nil {
				return fmt.Errorf("error reading only-cordoned flag: %s", err)
			}

			inverse, err := cmd.Flags().GetBool("inverse")
			if err != nil {
				return fmt.Errorf("error reading inverse flag: %s", err)
			}

			labels, err := cmd.Flags().GetStringSlice("selector")
			if err != nil {
				return fmt.Errorf("error reading queue label selector: %s", err)
			}

			priorityClasses, err := cmd.Flags().GetStringSlice("priority-classes")
			if err != nil {
				return fmt.Errorf("error reading priority-classes flag: %s", err)
			}

			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("error reading dry-run flag: %s", err)
			}

			if len(queues) > 0 && len(labels) > 0 {
				return fmt.Errorf("you can select either with a set of queue names or a set of queue labels, but not both")
			} else if len(queues) == 0 && len(labels) == 0 {
				// This check makes accidentally preempting all jobs far less likely
				return fmt.Errorf("you must narrow down queue selection with either queue names or labels")
			}

			return a.PreemptOnQueues(&armadactl.QueueQueryArgs{
				InQueueNames:      queues,
				ContainsAllLabels: labels,
				InvertResult:      inverse,
				OnlyCordoned:      onlyCordoned,
			}, priorityClasses, dryRun)
		},
	}
	cmd.Flags().StringSliceP("priority-classes", "p", []string{}, "Jobs matching the provided priority classes will be preempted.")
	cmd.Flags().StringSliceP("selector", "l", []string{}, "Select queues to preempt by label.")
	cmd.Flags().Bool("inverse", false, "Inverts result to preempt all queues that don't match the specified criteria. Defaults to false.")
	cmd.Flags().Bool("only-cordoned", false, "Only preempts queues that are cordoned. Defaults to false.")
	cmd.Flags().Bool("dry-run", false, "Prints out queues on which jobs will be preempted. Defaults to false.")

	return cmd
}
