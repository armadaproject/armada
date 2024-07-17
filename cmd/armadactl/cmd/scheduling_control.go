package cmd

import (
	"fmt"
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func cordon() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "cordon",
		Short: "Pause scheduling by resource",
		Long:  "Pause scheduling by resource. Supported: queue",
	}
	cmd.AddCommand(cordonQueue(a))
	return cmd
}

func uncordon() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "uncordon",
		Short: "Resume scheduling by resource",
		Long:  "Resume scheduling by resource. Supported: queue",
	}
	cmd.AddCommand(uncordonQueue(a))
	return cmd
}

func cordonQueue(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Pause scheduling for select queues",
		Long:  "Pause scheduling for select queues. This can be achieved by queue name or by labels.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			matchQueues, err := cmd.Flags().GetStringSlice("match-queues")
			if err != nil {
				return fmt.Errorf("error reading queue selection: %s", err)
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

			if len(matchQueues) == 0 && len(matchLabels) == 0 {
				return fmt.Errorf("either match-queues or match-labels must be set to narrow down queues to cordon")
			}

			return a.CordonQueues(matchQueues, matchLabels, dryRun, inverse)
		},
	}
	cmd.Flags().StringSliceP("match-queues", "q", []string{}, "Provide a comma separated list of queues you'd like to pause scheduling for. Defaults to empty.")
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling paused. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")
	return cmd
}

func uncordonQueue(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Resume scheduling for select queues",
		Long:  "Resume scheduling for select queues. This can be achieved by queue name or by labels.",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			matchQueues, err := cmd.Flags().GetStringSlice("match-queues")
			if err != nil {
				return fmt.Errorf("error reading queue selection: %s", err)
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

			if len(matchQueues) == 0 && len(matchLabels) == 0 {
				return fmt.Errorf("either match-queues or match-labels must be set to narrow down queues to uncordon")
			}

			return a.UncordonQueues(matchQueues, matchLabels, dryRun, inverse)
		},
	}
	cmd.Flags().StringSliceP("match-queues", "q", []string{}, "Provide a comma separated list of queues you'd like to resume scheduling for. Defaults to empty.")
	cmd.Flags().StringSliceP("match-labels", "l", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling resumed. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")
	return cmd
}
