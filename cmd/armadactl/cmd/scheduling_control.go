package cmd

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

func controlSchedulingCmd() *cobra.Command {
	a := armadactl.New()
	cmd := &cobra.Command{
		Use:   "control-scheduling",
		Short: "Pause or resume scheduling for select queues",
		Long:  "Pause or resume scheduling for select queues. Supported: pause, resume",
		PreRunE: func(cmd *cobra.Command, args []string) error {
			return initParams(cmd, a.Params)
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			filePath, _ := cmd.Flags().GetString("file")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			return a.CreateResource(filePath, dryRun)
		},
	}
	cmd.AddCommand(pauseCmd(a))
	cmd.AddCommand(resumeCmd(a))
	return cmd
}

func pauseCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pause",
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

			return a.PauseScheduling(matchQueues, matchLabels, dryRun, inverse)
		},
	}
	cmd.Flags().StringSlice("match-queues", []string{}, "Provide a comma separated list of queues you'd like to pause scheduling for. Defaults to empty.")
	cmd.Flags().StringSlice("match-labels", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling paused. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")
	return cmd
}

func resumeCmd(a *armadactl.App) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "resume",
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

			return a.ResumeScheduling(matchQueues, matchLabels, dryRun, inverse)
		},
	}
	cmd.Flags().StringSlice("match-queues", []string{}, "Provide a comma separated list of queues you'd like to resume scheduling for. Defaults to empty.")
	cmd.Flags().StringSlice("match-labels", []string{}, "Provide a comma separated list of labels. Queues matching all provided labels will have scheduling resumed. Defaults to empty.")
	cmd.Flags().Bool("inverse", false, "Select all queues which do not match the provided parameters")
	cmd.Flags().Bool("dry-run", false, "Show selection of queues that will be modified in this operation")
	return cmd
}
