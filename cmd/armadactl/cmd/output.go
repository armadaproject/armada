package cmd

import (
	"github.com/spf13/cobra"

	"github.com/armadaproject/armada/internal/armadactl"
)

// withOutputFlag adds an optional --output/-o flag to a command
func withOutputFlag(cmd *cobra.Command, a *armadactl.App) *cobra.Command {
	cmd.Flags().StringP("output", "o", "", "yaml or json. Defaults to the command's usual output.")
	prev := cmd.PreRunE
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		if prev != nil {
			if err := prev(cmd, args); err != nil {
				return err
			}
		}
		output, err := cmd.Flags().GetString("output")
		if err != nil {
			return err
		}
		a.Params.Output, err = armadactl.ParseOutputFormat(output)
		return err
	}
	return cmd
}
