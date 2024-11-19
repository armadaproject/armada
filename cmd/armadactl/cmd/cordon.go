package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/cmd/armadactl/cmd/utils"
	"github.com/armadaproject/armada/internal/armadactl"
	"github.com/armadaproject/armada/internal/common/slices"
)

func cordon() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "cordon",
		Short: "Pause scheduling by resource",
		Long:  "Pause scheduling by resource. Supported: queue, queues, executor",
	}
	cmd.AddCommand(cordonQueues(a))
	cmd.AddCommand(cordonExecutor(a))
	return cmd
}

func uncordon() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "uncordon",
		Short: "Resume scheduling by resource",
		Long:  "Resume scheduling by resource. Supported: queue, queues, executor",
	}
	cmd.AddCommand(uncordonQueues(a))
	cmd.AddCommand(uncordonExecutor(a))
	return cmd
}

func cordonQueues(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "queues <queue_1> <queue_2> <queue_3> ...",
		Aliases: []string{"queue"},
		Short:   "Pause scheduling for select queues",
		Long:    "Pause scheduling for select queues. This can be achieved either by queue names or by labels.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, queues []string) error {
			errs := slices.Filter(slices.Map(queues, utils.QueueNameValidation), func(err error) bool { return err != nil })
			if len(errs) > 0 {
				return fmt.Errorf("provided queue name invalid: %s", errs[0])
			}

			matchLabels, err := cmd.Flags().GetStringSlice("match-labels")
			if err != nil {
				return fmt.Errorf("error reading label selection: %s", err)
			}

			inverse, err := cmd.Flags().GetBool("inverse")
			if err != nil {
				return fmt.Errorf("error reading inverse flag: %s", err)
			}

			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("error reading dry-run flag: %s", err)
			}

			if len(queues) == 0 && len(matchLabels) == 0 {
				return fmt.Errorf("either queue names or match-labels must be set to determine queues to cordon")
			} else if len(queues) > 0 && len(matchLabels) > 0 {
				return fmt.Errorf("you can cordon by either a set of queue names or a set of queue labels, but not both")
			}

			return a.CordonQueues(&armadactl.QueueQueryArgs{
				InQueueNames:      queues,
				ContainsAllLabels: matchLabels,
				InvertResult:      inverse,
				OnlyCordoned:      false,
			}, dryRun)
		},
	}
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling paused. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")

	return cmd
}

func uncordonQueues(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "queues <queue_1> <queue_2> <queue_3> ...",
		Aliases: []string{"queue"},
		Short:   "Resume scheduling for select queues",
		Long:    "Resume scheduling for select queues. This can be achieved either by queue names or by labels.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, queues []string) error {
			errs := slices.Filter(slices.Map(queues, utils.QueueNameValidation), func(err error) bool { return err != nil })
			if len(errs) > 0 {
				return fmt.Errorf("provided queue name invalid: %s", errs[0])
			}

			matchLabels, err := cmd.Flags().GetStringSlice("match-labels")
			if err != nil {
				return fmt.Errorf("error reading label selection: %s", err)
			}

			inverse, err := cmd.Flags().GetBool("inverse")
			if err != nil {
				return fmt.Errorf("error reading inverse flag: %s", err)
			}

			dryRun, err := cmd.Flags().GetBool("dry-run")
			if err != nil {
				return fmt.Errorf("error reading dry-run flag: %s", err)
			}

			if len(queues) == 0 && len(matchLabels) == 0 {
				return fmt.Errorf("either queue names or match-labels must be set to determine queues to uncordon")
			} else if len(queues) > 0 && len(matchLabels) > 0 {
				return fmt.Errorf("you can uncordon by either a set of queue names or a set of queue labels, but not both")
			}

			return a.UncordonQueues(&armadactl.QueueQueryArgs{
				InQueueNames:      queues,
				ContainsAllLabels: matchLabels,
				InvertResult:      inverse,
				OnlyCordoned:      false,
			}, dryRun)
		},
	}
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling resumed. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")

	return cmd
}

func cordonExecutor(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executor <executor_name> <cordon_reason>",
		Short: "Pause scheduling on an executor",
		Long:  "Pause scheduling on an executor",
		Args:  cobra.ExactArgs(2),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			executorName := args[0]
			cordonReason := args[1]
			if executorName == "" {
				return fmt.Errorf("provided executor name is invalid: %s", executorName)
			} else if cordonReason == "" {
				return fmt.Errorf("provided cordon reason is invalid: %s", cordonReason)
			}

			return a.CordonExecutor(executorName, cordonReason)
		},
	}
	return cmd
}

func uncordonExecutor(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "executor <executor_name>",
		Short: "Resume scheduling on an executor",
		Long:  "Resume scheduling on an executor",
		Args:  cobra.ExactArgs(1),
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			executorName := args[0]
			if executorName == "" {
				return fmt.Errorf("provided executor name is invalid: %s", executorName)
			}

			return a.UncordonExecutor(executorName)
		},
	}
	return cmd
}
