package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/cmd/armadactl/cmd/utils"
	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/internal/common/slices"
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
		cancelNodeCmd(),
		cancelQueueCmd(),
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
		Use:   "executor <executor>",
		Short: "Cancels jobs on executor.",
		Long:  `Cancels jobs on executor with provided executor name, priority classes, queues, and pools.`,
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

			pools, err := cmd.Flags().GetStringSlice("pools")
			if err != nil {
				return fmt.Errorf("error reading pools flag: %s", err)
			}

			return a.CancelOnExecutor(onExecutor, queues, priorityClasses, pools)
		},
	}

	cmd.Flags().StringSliceP(
		"queues",
		"q",
		[]string{},
		"Cancel jobs on executor matching the specified queue names. If no queues are provided, jobs across all queues will be cancelled. Provided queues should be comma separated, as in the following example: queueA,queueB,queueC.",
	)
	cmd.Flags().StringSliceP("priority-classes", "p", []string{}, "Cancel jobs on executor matching the specified priority classes. Provided priority classes should be comma separated, as in the following example: armada-default,armada-preemptible.")
	cmd.Flags().StringSlice("pools", []string{}, "Cancel jobs on executor matching the specified pools. If no pools are provided, jobs across all pools will be cancelled. Provided pools should be comma separated, as in the following example: pool-a,pool-b.")
	return cmd
}

func cancelNodeCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "node <name>",
		Short: "Cancels jobs on node for specified executor.",
		Long:  `Cancels jobs on node for executor with provided executor name, priority classes and queues.`,
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.MarkFlagRequired("priority-classes"); err != nil {
				return fmt.Errorf("error marking priority-class flag as required: %s", err)
			}
			if err := cmd.MarkFlagRequired("executor"); err != nil {
				return fmt.Errorf("error marking executor flag as required: %s", err)
			}
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			node := args[0]

			priorityClasses, err := cmd.Flags().GetStringSlice("priority-classes")
			if err != nil {
				return fmt.Errorf("error reading priority-class selection: %s", err)
			}

			queues, err := cmd.Flags().GetStringSlice("queues")
			if err != nil {
				return fmt.Errorf("error reading queue selection: %s", err)
			}

			executor, err := cmd.Flags().GetString("executor")
			if err != nil {
				return fmt.Errorf("error reading executor flag: %s", err)
			}

			return a.CancelOnNode(node, executor, queues, priorityClasses)
		},
	}

	cmd.Flags().StringSliceP(
		"queues",
		"q",
		[]string{},
		"Cancel jobs on node for specified executor matching the specified queue names. If no queues are provided, jobs across all queues will be cancelled. Provided queues should be comma separated, as in the following example: queueA,queueB,queueC.",
	)
	cmd.Flags().StringSliceP(
		"priority-classes",
		"p",
		[]string{},
		"Cancel jobs on node for specified executor matching the specified priority classes. Provided priority classes should be comma separated, as in the following example: armada-default,armada-preemptible.",
	)
	cmd.Flags().StringP(
		"executor",
		"e",
		"",
		"Name of the executor that owns the node where jobs will be cancelled.",
	)
	return cmd
}

func cancelQueueCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:     "queues <queue_1> <queue_2> <queue_3> ...",
		Short:   "Cancels jobs on queues.",
		Long:    `Cancels jobs on queues with provided name, priority classes, job states, and pools. Allows selecting of queues by label or name, one of which must be provided. All flags with multiple values must be comma separated.`,
		Aliases: []string{"queue"},
		PreRunE: func(cmd *cobra.Command, args []string) error {
			if err := cmd.MarkFlagRequired("job-states"); err != nil {
				return err
			}
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

			jobStates, err := cmd.Flags().GetStringSlice("job-states")
			if err != nil {
				return fmt.Errorf("error reading job-states flag: %s", err)
			}

			var activeJobStates []utils.ActiveJobState
			for _, state := range jobStates {
				activeState, err := utils.ActiveJobStateFromString(state)
				if err != nil {
					return fmt.Errorf("error determining active job state corresponding to %s: %s", state, err)
				}
				activeJobStates = append(activeJobStates, activeState)
			}

			priorityClasses, err := cmd.Flags().GetStringSlice("priority-classes")
			if err != nil {
				return fmt.Errorf("error reading priority-classes flag: %s", err)
			}

			pools, err := cmd.Flags().GetStringSlice("pools")
			if err != nil {
				return fmt.Errorf("error reading pools flag: %s", err)
			}

			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("error reading dry-run flag: %s", err)
			}

			if len(queues) > 0 && len(labels) > 0 {
				return fmt.Errorf("queues can be selected either with a set of names or a set of labels, but not both")
			} else if len(queues) == 0 && len(labels) == 0 {
				// This check makes accidentally cancelling all jobs far less likely
				return fmt.Errorf("queue selection must be narrowed down either by names or by labels")
			}

			return a.CancelOnQueues(&armadactl.QueueQueryArgs{
				InQueueNames:      queues,
				ContainsAllLabels: labels,
				InvertResult:      inverse,
				OnlyCordoned:      onlyCordoned,
			}, priorityClasses, pools, activeJobStates, dryRun)
		},
	}
	cmd.Flags().StringSliceP("job-states", "s", []string{}, "Jobs in the provided job states will be cancelled. Allowed values: queued,leased,pending,running.")
	cmd.Flags().StringSliceP("priority-classes", "p", []string{}, "Jobs matching the provided priority classes will be cancelled.")
	cmd.Flags().StringSlice("pools", []string{}, "Cancel jobs matching the specified pools. If no pools are provided, jobs across all pools will be cancelled.")
	cmd.Flags().StringSliceP("selector", "l", []string{}, "Select queues by label.")
	cmd.Flags().Bool("inverse", false, "Inverts result to cancel all queues that don't match the specified criteria. Defaults to false.")
	cmd.Flags().Bool("only-cordoned", false, "Only cancels queues that are cordoned. Defaults to false.")
	cmd.Flags().Bool("dry-run", false, "Prints out queues on which jobs will be cancelled. Defaults to false.")

	return cmd
}
